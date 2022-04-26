import test from 'ava'
import Redis from 'ioredis'
import { worker, stream } from './index.js'

let redis = new Redis()

// test('should add a job with no data to stream', async (t) => {
//   let q = stream({ redis })
//   let id = await q.addJob('email')
//   t.regex(id, /\d+-\d/)
// })

// test.serial('should create and destroy consumer group', async (t) => {
//   let w = worker({ redis, stream: 'email' })
//   await w.createConsumerGroupIfNotExists()
//   await w.destroyConsumerGroup()
//   t.pass()
// })

test('should process job', async (t) =>
  // eslint-disable-next-line
  new Promise(async (resolve) => {
    let w = worker({
      redis,
      stream: 'email',
      encode: JSON.stringify,
      decode: JSON.parse,
    })
    let q = stream({ redis, encode: JSON.stringify })
    let sentData = { from: 'david@example.com' }
    await q.addJob('email', sentData)
    await w.createConsumerGroupIfNotExists()
    await w.startWorker((receivedData) => {
      t.deepEqual(receivedData, sentData)
      resolve()
    })
  }))
// let handler = (data, signal) =>
//   new Promise((resolve, reject) => {
//     signal.addEventListener('abort', () => {
//       reject('aborted')
//     })
//     debug(data)
//     resolve('done')
//   })

// console.log(await destroyConsumerGroup('email'))
// console.log(await redis.del('stream:email'))
// console.log(
//   await createConsumerGroupIfNotExists('email', { readFromStart: true })
// )
// console.log(await addJob('email', { name: 'Bob' }))
// await startWorker('email', handler, { retries: expBackoff(1) })
// await getEntryStats('email', '1639515173383-1').then(console.log)
