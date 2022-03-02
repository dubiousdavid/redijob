import _ from 'lodash/fp.js'
import ms from 'ms'
import { pack } from 'msgpackr'
import _debug from 'debug'

let debug = _debug('redijob:queue')

export default ({ redis, prefix = 'redijob', encode = pack }) => {
  let getQueuePrefix = (queue) =>
    prefix ? `${prefix}:queue:${queue}` : `queue:${queue}`

  let redisArrayToCmd = ([cmd, ...vals]) => redis[cmd](...vals)

  let _addJob = (queue, data, options) => [
    'xadd',
    getQueuePrefix(queue),
    '*',
    'data',
    encode(data),
    ...(!_.isEmpty(options) ? ['options', encode(options)] : []),
  ]

  let addJob = (queue, data, options) => {
    debug('Added job to queue %s', queue)
    return redisArrayToCmd(_addJob(queue, data, options))
  }

  let _addTopology = (topology) => {
    let { queue, data, options } = _.head(topology)
    return (
      queue && _addJob(queue, data, { ...options, topology: _.tail(topology) })
    )
  }

  // Format: [{queue, data, options},{queue, data, options}]
  // Options limited to timeout, retries
  let addTopology = (topology) => redisArrayToCmd(_addTopology(topology))

  let trimQueueByAge = (queue, ageMs = ms('1d')) =>
    redis.xtrim(getQueuePrefix(queue), 'MINID', ageMs)

  let trimQueueByLength = (queue, length = 1e4) =>
    redis.xtrim(getQueuePrefix(queue), 'MAXLEN', '~', length)

  return {
    addJob,
    addTopology,
    _addTopology,
    trimQueueByAge,
    trimQueueByLength,
  }
}
