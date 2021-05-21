package io.hevo.scheduler.lock

import io.hevo.scheduler.util.Util
import redis.clients.jedis.JedisPool

import scala.util.Using

class RedisBasedLock(jedisPool: JedisPool) extends Lock {

  override def acquire(lockId: String, ttl: Int): Boolean = {
    var reply: String = null
    Util.throwOnError(Using(this.jedisPool.getResource) {
      resource => reply = resource.set(lockId, lockId, "NX", "PX", ttl)
    })
    "OK".equals(reply)
  }

  override def release(lockId: String): Unit = {
    Util.throwOnError(Using(this.jedisPool.getResource) {
      resource => resource.del(lockId)
    })
  }
}