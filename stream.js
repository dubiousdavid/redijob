import _ from 'lodash/fp.js'
import ms from 'ms'
import { pack } from 'msgpackr'
import _debug from 'debug'

let debug = _debug('redijob:stream')

export default ({ redis, prefix = 'redijob', encode = pack }) => {
  let getStreamPrefix = (stream) =>
    prefix ? `${prefix}:stream:${stream}` : `stream:${stream}`

  let redisArrayToCmd = ([cmd, ...vals]) => redis[cmd](...vals)

  let _addJob = (stream, data, options) => [
    'xadd',
    getStreamPrefix(stream),
    '*',
    'data',
    encode(data),
    ...(!_.isEmpty(options) ? ['options', encode(options)] : []),
  ]

  let addJob = (stream, data, options) => {
    debug('Added job to stream %s', stream)
    return redisArrayToCmd(_addJob(stream, data, options))
  }

  let _addTopology = (topology) => {
    let { stream, data, options } = _.head(topology)
    return (
      stream && _addJob(stream, data, { ...options, topology: _.tail(topology) })
    )
  }

  // Format: [{stream, data, options},{stream, data, options}]
  // Options limited to timeout, retries
  let addTopology = (topology) => redisArrayToCmd(_addTopology(topology))

  let trimStreamByAge = (stream, ageMs = ms('1d')) =>
    redis.xtrim(getStreamPrefix(stream), 'MINID', ageMs)

  let trimStreamByLength = (stream, length = 1e4) =>
    redis.xtrim(getStreamPrefix(stream), 'MAXLEN', '~', length)

  return {
    addJob,
    addTopology,
    _addTopology,
    trimStreamByAge,
    trimStreamByLength,
  }
}
