package io.hevo.scheduler.config

class WorkerConfig(_appId: String) {

  /**
   * Host app identifier. Unique per JVM
   */
  val appId: String = _appId

  var workers: Int = 5
  /**
   * The number of tasks picked up in one go will be workers * pickupFactor
   */
  var pickupFactor: Int = 3
  var batchUpdates: Boolean = true
  /**
   * Sync the task updates to the db in batches at this frequency (in seconds)
   * This is applicable only when batchUpdates has been set as true
   */
  var batchUpdateFrequency: Long = 3
  /**
   * Maximum time in future to pick the tasks from (in seconds)
   */
  var maxLookAheadTime: Int = 3
  /**
   * Attempt cleanup of "stuck" objects proactively at this frequency (in seconds)
   */
  var cleanupFrequency: Int = 30 * 60

  def workers(workers: Int): WorkerConfig = {
    this.workers = workers
    this
  }

  def pickupFactor(pickupFactor: Int): WorkerConfig = {
    this.pickupFactor = pickupFactor
    this
  }

  def batchUpdates(batchUpdates: Boolean): WorkerConfig = {
    this.batchUpdates = batchUpdates
    this
  }

  def batchUpdateFrequency(batchUpdateFrequency: Int): WorkerConfig = {
    this.batchUpdateFrequency = batchUpdateFrequency
    this
  }

  def maxLookAheadTime(maxLookAheadTime: Int): WorkerConfig = {
    this.maxLookAheadTime = maxLookAheadTime
    this
  }
}
