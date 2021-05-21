package io.hevo.scheduler.config

import io.hevo.scheduler.lock.Lock
import javax.sql.DataSource

class SchedulerConfig(val dataSource: DataSource, val _workerConfig: WorkerConfig) {
  var tablePrefix: String = ""
  var lock: Lock = _
  /**
   * In Seconds
   */
  var pollFrequency: Int = 1

  var workerConfig: WorkerConfig = _workerConfig

  def redisPool(lock: Lock): SchedulerConfig = {
    this.lock = lock
    this
  }

  def pollFrequency(pollFrequency: Int): SchedulerConfig = {
    this.pollFrequency = pollFrequency
    this
  }

  def tablePrefix(tablePrefix: String): SchedulerConfig = {
    this.tablePrefix = tablePrefix
    this
  }

}