import _ from 'lodash/fp.js'
import ms from 'ms'
import timeoutSignal from 'timeout-signal'
import { addMilliseconds, subMilliseconds } from 'date-fns'
import { unpack, pack } from 'msgpackr'
import _debug from 'debug'
import { swallowErrorIf } from './util/error.js'
import initQueue from './queue.js'

let debug = _debug('redijob:worker')

export default ({
  queue,
  redis,
  prefix = 'redijob',
  encode = pack,
  decode = unpack,
}) => {
  let q = initQueue({ redis, prefix, encode })
  let queuePrefix = prefix ? `${prefix}:queue:${queue}` : `queue:${queue}`
  debug('Queue prefix %s', queuePrefix)

  let createConsumerGroup = ({ readFromStart = false } = {}) =>
    redis.xgroup(
      'CREATE',
      queuePrefix,
      'workerGroup',
      // $ indicates the ID of the last item in the stream
      readFromStart ? 0 : '$',
      'MKSTREAM'
    )

  // Create consumer group if it doesn't exist
  let createConsumerGroupIfNotExists = (options) =>
    createConsumerGroup(options).catch(
      swallowErrorIf([/Consumer Group name already exists/])
    )

  let destroyConsumerGroup = () =>
    redis.xgroup('DESTROY', queuePrefix, 'workerGroup')

  let getPendingMessages = (workerId, count = 1000) =>
    redis
      .xpending(queuePrefix, 'workerGroup', '-', '+', count, workerId)
      .then(_.map(_.head))

  let getEntryStats = (id) =>
    redis.xpending(queuePrefix, 'workerGroup', id, id, 1).then(
      ([[, , elapsedMs, numDeliveries] = []]) =>
        numDeliveries && {
          elapsedMs,
          numDeliveries,
        }
    )

  let recordHeartbeat = (workerId) =>
    redis.zadd(`${queuePrefix}:workers`, new Date().getTime(), workerId)

  let getWorkerId = () => redis.client('ID').then((id) => `worker-${id}`)

  let _recordEvent = (id, obj) => [
    'rpush',
    `${queuePrefix}:${id}:events`,
    encode({ timestamp: new Date().getTime(), ...obj }),
  ]

  let handleError = async ({ error, data, workerId, id, options, delayed }) => {
    options.logger.warn(error)
    // Get stats for job
    let { elapsedMs, numDeliveries } = await getEntryStats(id)
    debug('elapsedMs %d, numDeliveries %d', elapsedMs, numDeliveries)
    let attemptThresholdReached =
      numDeliveries === _.size(options.retries) || _.isEmpty(options.retries)
    let commands = [
      // Record error
      _recordEvent(id, {
        data,
        stacktrace: error.stack,
        workerId,
        options: _.omit('logger', options),
        numDeliveries,
        attemptThresholdReached,
      }),
      // Notify job errored
      [
        'publish',
        `${queuePrefix}:errored`,
        encode({ id, error, elapsedMs, numDeliveries }),
      ],
    ]
    // Attempt threshold reached
    if (attemptThresholdReached) {
      debug('Attempt threshold reached')
      commands.push(
        // Ack message as processed
        ['xack', queuePrefix, 'workerGroup', id],
        // Notify job failed
        ['publish', `${queuePrefix}:failed`, encode({ id, error })]
      )
      if (delayed) {
        // Remove from delayed set
        commands.push(['zrem', `${queuePrefix}:delayed`, id])
      }
    }
    // Retry
    else {
      let nextRetryMs = options.retries[numDeliveries - 1]
      debug('nextRetryMs %d', nextRetryMs)
      // Add job to delayed set
      commands.push([
        'zadd',
        `${queuePrefix}:delayed`,
        addMilliseconds(new Date(), nextRetryMs),
        id,
      ])
    }
    await redis.multi(commands).exec()
  }

  let getDelayed = async (workerId) => {
    // Get the first id where the delayed timestamp is <= now
    let id = await redis
      .zrangebyscore(
        `${queuePrefix}:delayed`,
        '-inf',
        new Date().getTime(),
        'LIMIT',
        0,
        1
      )
      .then(_.head)
    if (id) {
      // Get entry by id
      let entry = await getEntry(workerId, { id })
      return { ...entry, delayed: true }
    }
  }

  let defaultOptions = {
    timeout: ms('1m'),
    retries: [],
    emitHeartbeatEveryMs: ms('1m'),
    workerConsideredDeadMs: ms('10m'),
    claimJobsFromDeadWorkerMs: ms('5m'),
    keepEventsForMs: ms('1d'),
    logger: console,
  }

  let startWorker = async (handler, options) => {
    options = _.defaults(defaultOptions, options)
    debug('Starting worker with options %O', options)
    // Get an ID
    let workerId = await getWorkerId()
    debug('Worker id %s', workerId)
    // Record first heartbeat
    await recordHeartbeat(workerId)
    // Periodically record heartbeat
    setInterval(() => recordHeartbeat(workerId), options.emitHeartbeatEveryMs)
    // Periodically attempt to claim jobs from a dead worker
    setInterval(
      () => claimJobsFromDeadWorker(options.workerConsideredDeadMs, workerId),
      options.claimJobsFromDeadWorkerMs
    )
    // Process jobs
    /* eslint-disable */
    while (true) {
      let entry = (await getDelayed(workerId)) || (await getEntry(workerId))
      if (entry) {
        debug('Processing job %O', entry)
        let { id, data, options: entryOptions, delayed } = entry
        options = _.merge(options, entryOptions)
        debug('Job options %O', options)
        try {
          await redis
            .multi([
              // Record event
              _recordEvent(id, { processing: true }),
              [
                'pexpire',
                `${queuePrefix}:${id}:events`,
                options.keepEventsForMs,
              ],
              // Notify job is processing
              ['publish', `${queuePrefix}:processing`, id],
            ])
            .exec()
          let signal = timeoutSignal(options.timeout)
          let result = await handler(data, signal)
          let commands = [
            // Ack message
            ['xack', queuePrefix, 'workerGroup', id],
            // Record event to list
            _recordEvent(id, { completed: true }),
            // Notify job completed
            ['publish', `${queuePrefix}:completed`, id],
          ]
          // Add next job in topology if one exists
          if (!_.isEmpty(options.topology)) {
            let job = q._addTopology(
              // Merge result data with next job data
              _.update('0.data', _.merge(result), options.topology)
            )
            if (job) {
              commands.push(job)
            }
          }
          if (entry.delayed) {
            // Remove delayed
            commands.push(['zrem', `${queuePrefix}:delayed`, id])
          }
          await redis.multi(commands).exec()
        } catch (error) {
          await handleError({ error, data, workerId, id, options, delayed })
        }
      }
    }
  }

  let parseStreamEntries = (entries) => {
    let [id, kvs] = _.get('0.1.0', entries)
    return { id, ...fromKVPairs(kvs) }
  }

  // Get entry from the stream, blocking up to blockMs
  let getEntry = async (workerId, { blockMs = 1000, id = '>' } = {}) => {
    let data = await redis.xreadgroup(
      'GROUP',
      'workerGroup',
      workerId,
      'COUNT',
      1,
      'BLOCK',
      blockMs,
      'STREAMS',
      queuePrefix,
      id
    )
    return data && parseStreamEntries(data)
  }

  // let toKVPairs = _.flow(F.compactObject, _.mapValues(encode), _.toPairs)
  let fromKVPairs = _.flow(_.chunk(2), _.fromPairs, _.mapValues(decode))

  let claimAndRemoveDeadWorker = (
    queue,
    deadWorkerId,
    newWorkerId,
    messageIds
  ) =>
    redis
      .multi([
        [
          'xclaim',
          queuePrefix,
          'workerGroup',
          newWorkerId,
          ms('1h'),
          ...messageIds,
        ],
        // Remove dead worker
        ['zrem', `${queuePrefix}:workers`, deadWorkerId],
      ])
      .exec()

  let claimJobsFromDeadWorker = async (
    queue,
    workerConsideredDeadMs,
    newWorkerId
  ) => {
    // Get dead worker
    let deadWorkerId = await redis.zrangebyscore(
      `${queuePrefix}:workers`,
      '-inf',
      subMilliseconds(new Date(), workerConsideredDeadMs),
      'LIMIT',
      0,
      1
    )
    if (deadWorkerId) {
      // Get pending messages for worker
      let messageIds = await getPendingMessages(queue, deadWorkerId)
      if (messageIds) {
        await claimAndRemoveDeadWorker(
          queue,
          deadWorkerId,
          newWorkerId,
          messageIds
        )
      }
    }
  }

  return {
    createConsumerGroup,
    createConsumerGroupIfNotExists,
    destroyConsumerGroup,
    startWorker,
  }
}
