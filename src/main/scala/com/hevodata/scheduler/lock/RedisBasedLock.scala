package com.hevodata.scheduler.lock

import redis.clients.jedis.JedisPool
import redis.clients.jedis.Jedis

/**
 * A light-weight redis based implementation of the Lock trait
 */
class RedisBasedLock(namespace: String = "", jedisPool: JedisPool) extends Lock {

  override def acquire(lockId: String, ttlSeconds: Int): Boolean = {
    var reply: String = null
    var resource: Jedis = null
    try {
      resource = this.jedisPool.getResource
      val effectiveLockId = namespace + lockId
      reply = resource.set(effectiveLockId, effectiveLockId, "NX", "EX", ttlSeconds)
    }
    finally {
      resource.close()
    }
    "OK".equals(reply)
  }

  override def release(lockId: String): Unit = {
    var resource: Jedis = null
    try {
      resource = this.jedisPool.getResource
      val effectiveLockId = namespace + lockId
      resource.del(effectiveLockId)
    }
    finally {
      resource.close()
    }
  }
}