/*
 * Data Structures:
 *
 * 1. queue:[queue] - Stream representing a logical queue.
 * 2. queue:[queue]:workers - Sorted set of workers per queue, sorted by heartbeat timestamp.
 * 3. queue:[queue]:delayed - Sorted set of delayed jobs per queue, sorted by delayed timestamp.
 * 4. queue:[queue]:completed - Channel of completed jobs per queue.
 * 5. queue:[queue]:errored - Channel of errored jobs per queue.
 * 6. queue:[queue]:failed - Channel of failed jobs per queue.
 * 7. queue:[queue]:[id]:events - List of events per job.
 */

export { default as worker } from './worker.js'
export { default as queue } from './queue.js'
