package com.hevodata.scheduler.core.service

import com.hevodata.scheduler.lock.Lock
import com.hevodata.scheduler.util.Util
import org.slf4j.LoggerFactory

class LockHandler(locker: Option[Lock]) {
  private val LOG = LoggerFactory.getLogger(classOf[LockHandler])
  /**
   * It makes LockHandler.attempts number of attempts to acquire the lock in random gaps of LockHandler.sleepOver ms each
   * @return true if there is no Lock to be acquired or the lock acquisition was successful
   */
  def acquire(lockId: String, ttlSeconds: Int): Boolean = `{
    var acquired: Boolean = true
    if(locker.iterator.size > 0) {
      acquired = false
      (0 to LockHandler.attempts).iterator.takeWhile(_ => !acquired).foreach(iteration => {
        acquired = locker.get.acquire(lockId, ttlSeconds)
        if(!acquired) {
          Thread.sleep(Util.getRandom(LockHandler.sleepOver))
        }
      })
    }
    acquired
  }

  def release(lockId: String): Unit = {
    if (locker.isDefined) {
      locker.get.release(lockId)
    }
  }
}

object LockHandler {
  val attempts: Int = 3
  val sleepOver: (Int, Int) = (10, 41)
}
