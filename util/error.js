import _ from 'lodash/fp.js'

let condMatch = (e) => (cond) => {
  if (_.isRegExp(cond)) {
    return cond.test(e.name) || cond.test(e.message)
  }
  if (_.isString(cond)) {
    return cond === e.name || cond === e.message
  }
  if (_.isFunction(cond)) {
    return e instanceof cond
  }
}

export let swallowErrorIf = _.curry((conditions, err) => {
  let _err = _.isString(err) ? { message: err } : err
  if (!_.some(condMatch(_err), conditions)) {
    throw err
  }
})

export let matchError = _.curry((conditions, err) => {
  let _err = _.isString(err) ? { message: err } : err
  let match = _.find(_.flow(_.first, condMatch(_err)), conditions)
  if (match) {
    let [, fn] = match
    return fn(err)
  }
  throw err
})
