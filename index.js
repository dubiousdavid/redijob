/*
 * Data Structures:
 *
 * 1. stream:[stream] - Stream representing a logical queue.
 * 3. stream:[stream]:delayed - Sorted set of delayed jobs per stream, sorted by delayed timestamp.
 * 4. stream:[stream]:completed - Channel of completed jobs per stream.
 * 5. stream:[stream]:errored - Channel of errored jobs per stream.
 * 6. stream:[stream]:failed - Channel of failed jobs per stream.
 * 7. stream:[stream]:[id]:events - List of events per job.
 */

export { default as worker } from './worker.js'
export { default as stream } from './stream.js'
