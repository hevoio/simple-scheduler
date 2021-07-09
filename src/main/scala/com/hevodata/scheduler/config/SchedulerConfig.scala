package com.hevodata.scheduler.config

import com.hevodata.scheduler.lock.Lock
import javax.sql.DataSource

/**
 * @param dataSource An instance of the DataSource from which connections can be created.
 * @param _workerConfig See WorkerConfig
 */
class SchedulerConfig(val dataSource: DataSource, val _workerConfig: WorkConfig) {
  /**
   * An optional prefix to be applied to the name of database tables utilized by the scheduler
   */
  var tablePrefix: String = ""
  /**
   * If present, before trying to pick up tasks, a lock may be acquired among the contending scheduler instances
   * Recommended in the multi cluster mode
   */
  var lock: Lock = _
  /**
   * Frequency at which the main thread should poll to check for tasks available to be executed (In Seconds)
   */
  var pollFrequency: Int = 2

  var workerConfig: WorkConfig = _workerConfig

  def withLock(lock: Lock): SchedulerConfig = {
    this.lock = lock
    this
  }

  def withPollFrequency(pollFrequency: Int): SchedulerConfig = {
    this.pollFrequency = pollFrequency
    this
  }

  def withTablePrefix(tablePrefix: String): SchedulerConfig = {
    this.tablePrefix = tablePrefix
    this
  }
}