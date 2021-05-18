package io.hevo.scheduler.core.service

import io.hevo.scheduler.lock.Lock
import org.slf4j.LoggerFactory

class LockHandler(locker: Option[Lock]) {

  private val LOG = LoggerFactory.getLogger(classOf[LockHandler])

  /**
   * It makes 3 attempts to acquire the lock in gaps of 30ms each
   * @return true if there is no Lock to be acquired or the lock acquisition was successful
   */
  def acquire(lockId: String, ttl: Int): Boolean = {
    if(locker.isEmpty) {
      true
    }
    else {
      for(_ <- 0.to(LockHandler.attempts)) {
        if(locker.get.acquire(lockId, ttl)) {
          true
        } else {
          Thread.sleep(LockHandler.sleepOver)
        }
      }
      false
    }
  }

  def release(lockId: String): Unit = {
    try {
      if (locker.isDefined) {
        locker.get.release(lockId)
      }
    }
    catch {
      case e: Exception => LOG.error("Failed to release the lock: {}", lockId, e)
    }
  }
}

object LockHandler {
  val attempts: Int = 3
  val sleepOver: Int = 30
}
