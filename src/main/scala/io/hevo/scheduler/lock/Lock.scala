package io.hevo.scheduler.lock

/**
 * Lock to acquire before acquiring tasks to process
 */
trait Lock {
  def acquire(lockId: String, ttl: Int): Boolean
  def release(lockId: String): Unit
}
