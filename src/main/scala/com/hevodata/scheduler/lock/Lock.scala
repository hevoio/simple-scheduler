package com.hevodata.scheduler.lock

/**
 * Lock to acquire before acquiring tasks to process
 */
trait Lock {
  /**
   * Tries to acquire the lock
   * @param lockId Unique identifier to be used by all of the contending instances
   * @param ttl Duration for which the lock must be acquired (In seconds)
   * @return True if the lock was acquired successfully
   */
  def acquire(lockId: String, ttl: Int): Boolean

  /**
   * Releases the lock immediately
   * @param lockId Unique identifier to be used by all of the contending instances
   */
  def release(lockId: String): Unit
}
