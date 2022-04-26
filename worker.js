import _ from 'lodash/fp.js'
import ms from 'ms'
import timeoutSignal from 'timeout-signal'
import { addMilliseconds } from 'date-fns'
import { unpack, pack } from 'msgpackr'
import _debug from 'debug'
import { swallowErrorIf } from '../../util/error.js'
import initStream from './stream.js'

let debug = _debug('redijob:worker')

export default ({
  stream,
  redis,
  prefix = 'redijob',
  encode = pack,
  decode = unpack,
}) => {
  let q = initStream({ redis, prefix, encode })
  let streamPrefix = prefix ? `${prefix}:stream:${stream}` : `stream:${stream}`
  debug('Stream prefix %s', streamPrefix)

  let createConsumerGroup = ({ readFromStart = false } = {}) =>
    redis.xgroup(
      'CREATE',
      streamPrefix,
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
    redis.xgroup('DESTROY', streamPrefix, 'workerGroup')

  let getEntryStats = (id) =>
    redis.xpending(streamPrefix, 'workerGroup', id, id, 1).then(
      ([[, , elapsedMs, numDeliveries] = []]) =>
        numDeliveries && {
          elapsedMs,
          numDeliveries,
        }
    )

  let getWorkerId = () => redis.client('ID').then((id) => `worker-${id}`)

  let _recordEvent = (id, obj) => [
    'rpush',
    `${streamPrefix}:${id}:events`,
    encode({ timestamp: new Date().getTime(), ...obj }),
  ]

  let handleError = async ({ error, data, workerId, id, options }) => {
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
        `${streamPrefix}:errored`,
        encode({ id, error, elapsedMs, numDeliveries }),
      ],
    ]
    // Attempt threshold reached
    if (attemptThresholdReached) {
      debug('Attempt threshold reached')
      commands.push(
        // Ack message as processed
        ['xack', streamPrefix, 'workerGroup', id],
        // Notify job failed
        ['publish', `${streamPrefix}:failed`, encode({ id, error })]
      )
    }
    // Retry
    else {
      let nextRetryMs = options.retries[numDeliveries - 1]
      debug('nextRetryMs %d', nextRetryMs)
      // Add job to delayed set
      commands.push([
        'zadd',
        `${streamPrefix}:delayed`,
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
        `${streamPrefix}:delayed`,
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
      if (entry) {
        // Remove delayed
        await redis.zrem(`${streamPrefix}:delayed`, id)
        return entry
      }
    }
  }

  let defaultOptions = {
    retries: [],
    ackWaitTime: ms('1m'),
    claimJobsEveryMs: ms('1m'),
    keepEventsForMs: ms('1d'),
    logger: console,
  }

  let startWorker = async (handler, options) => {
    options = _.defaults(defaultOptions, options)
    debug('Starting worker with options %O', options)
    // Get an ID
    let workerId = await getWorkerId()
    debug('Worker id %s', workerId)
    // Periodically attempt to claim jobs from a dead worker
    setInterval(
      () => claimUnackedJobs(stream, workerId, options.ackWaitTime),
      options.claimJobsEveryMs
    )
    // Process jobs
    /* eslint-disable */
    while (true) {
      let entry = (await getDelayed(workerId)) || (await getEntry(workerId))
      if (entry) {
        debug('Processing job %O', entry)
        let { id, data, options: entryOptions } = entry
        options = _.merge(options, entryOptions)
        debug('Job options %O', options)
        try {
          await redis
            .multi([
              // Record event
              _recordEvent(id, { processing: true }),
              [
                'pexpire',
                `${streamPrefix}:${id}:events`,
                options.keepEventsForMs,
              ],
              // Notify job is processing
              ['publish', `${streamPrefix}:processing`, id],
            ])
            .exec()
          // NOTE: This may be unnecessary
          let signal = timeoutSignal(options.ackWaitTime)
          let result = await handler(data, signal)
          let commands = [
            // Ack message
            ['xack', streamPrefix, 'workerGroup', id],
            // Record event to list
            _recordEvent(id, { completed: true }),
            // Notify job completed
            ['publish', `${streamPrefix}:completed`, id],
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
          await redis.multi(commands).exec()
        } catch (error) {
          await handleError({ error, data, workerId, id, options })
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
      // NOTE: BLOCK is ignored if id is not >
      'BLOCK',
      blockMs,
      'STREAMS',
      streamPrefix,
      id
    )
    return data && parseStreamEntries(data)
  }

  // let toKVPairs = _.flow(F.compactObject, _.mapValues(encode), _.toPairs)
  let fromKVPairs = _.flow(_.chunk(2), _.fromPairs, _.mapValues(decode))

  const claimUnackedJobs = async (stream, newWorkerId, ackWaitTime) =>
    redis.xautoclaim(
      streamPrefix,
      'workerGroup',
      newWorkerId,
      ackWaitTime,
      '0-0',
      'COUNT',
      10
    )

  return {
    createConsumerGroup,
    createConsumerGroupIfNotExists,
    destroyConsumerGroup,
    startWorker,
  }
}
