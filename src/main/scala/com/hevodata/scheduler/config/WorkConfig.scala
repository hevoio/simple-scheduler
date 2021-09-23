package com.hevodata.scheduler.config

class WorkConfig(_appId: String) {

  /**
   * App identifier. This should be unique per JVM. Recommended value = host name
   */
  val appId: String = _appId

  /**
   * Number of workers that will be responsible for actually executing the tasks
   */
  var workers: Int = 5
  /**
   * The number of tasks picked up in one iteration of the scheduler poll will be workers * pickupFactor
   */
  var pickupFactor: Int = 3
  /**
   * If the Success/Failure updates need not be written to the synchronizing db immediately, but may be batch updated at short intervals
   * Recommended if the periodicity of the jobs > 5 seconds
   */
  var batchUpdates: Boolean = true
  /**
   * Sync the task updates to the db in batches at this frequency (in seconds). This should be larger than SchedulerConfig.pollFrequency
   * This is applicable only when batchUpdates has been set as true
   */
  var batchUpdateFrequency: Long = 5
  /**
   * Maximum time in future to pick the tasks from (in seconds)
   */
  var maxLookAheadTime: Int = 3
  /**
   * Attempt cleanup of "stuck" objects proactively at this frequency (in seconds)
   */
  var cleanupFrequency: Int = 5 * 60

  var logFailures: Boolean = true

  var shutDownWait: Long = 45

  /**
   * In case the next execution should be referenced to the current time. Default is false where the next execution is calculated in reference to the previous scheduled time
   * If true, the next execution is in reference to the current execution completion instant
   */
  var nextRelativeToNow: Boolean = false

  def cleanupFrequency(cleanupFrequency: Int): WorkConfig = {
    this.cleanupFrequency = cleanupFrequency
    this
  }

  def workers(workers: Int): WorkConfig = {
    this.workers = workers
    this
  }

  def pickupFactor(pickupFactor: Int): WorkConfig = {
    this.pickupFactor = pickupFactor
    this
  }

  def batchUpdates(batchUpdates: Boolean): WorkConfig = {
    this.batchUpdates = batchUpdates
    this
  }

  def batchUpdateFrequency(batchUpdateFrequency: Int): WorkConfig = {
    this.batchUpdateFrequency = batchUpdateFrequency
    this
  }

  def maxLookAheadTime(maxLookAheadTime: Int): WorkConfig = {
    this.maxLookAheadTime = maxLookAheadTime
    this
  }

  def nextRelativeToNow(nextRelativeToNow: Boolean): WorkConfig = {
    this.nextRelativeToNow = nextRelativeToNow
    this
  }

  def logFailures(logFailures: Boolean): WorkConfig = {
    this.logFailures = logFailures
    this
  }

  def shutDownWait(shutDownWait: Long): WorkConfig = {
    this.shutDownWait = shutDownWait
    this
  }

  def tasksToRequest(unfinishedTasks: Int): Int = workers * pickupFactor - unfinishedTasks
}
