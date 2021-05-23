package io.hevo.scheduler.core.service

import java.util.Date

import io.hevo.scheduler.lock.Lock
import io.hevo.scheduler.util.Util
import org.slf4j.LoggerFactory

class LockHandler(locker: Option[Lock]) {
  private val LOG = LoggerFactory.getLogger(classOf[LockHandler])
  /**
   * It makes LockHandler.attempts number of attempts to acquire the lock in random gaps of LockHandler.sleepOver ms each
   * @return true if there is no Lock to be acquired or the lock acquisition was successful
   */
  def acquire(lockId: String, ttl: Int): Boolean = {
    var acquired: Boolean = true
    if(locker.nonEmpty) {
      acquired = false
      (0 to LockHandler.attempts).iterator.takeWhile(_ => !acquired).foreach(iteration => {
        acquired = locker.get.acquire(lockId, ttl)
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
  val sleepOver: (Int, Int) = (10, 40)
}
