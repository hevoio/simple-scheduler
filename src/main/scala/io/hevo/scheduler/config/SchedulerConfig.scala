package io.hevo.scheduler.config

import io.hevo.scheduler.lock.Lock
import org.jdbi.v3.core.Jdbi

class SchedulerConfig(val orm: Jdbi) {
  var lock: Lock = _
  /**
   * In Seconds
   */
  var pollFrequency: Int = 1

  var workerConfig: WorkerConfig = new WorkerConfig

  def redisPool(lock: Lock): SchedulerConfig = {
    this.lock = lock
    this
  }

  def pollFrequency(pollFrequency: Int): SchedulerConfig = {
    this.pollFrequency = pollFrequency
    this
  }

  def workerConfig(workerConfig: WorkerConfig): SchedulerConfig = {
    this.workerConfig = workerConfig
    this
  }
}