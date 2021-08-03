package com.hevodata.scheduler.lock

import redis.clients.jedis.JedisPool
import redis.clients.jedis.Jedis

/**
 * A light-weight redis based implementation of the Lock trait
 */
class RedisBasedLock(jedisPool: JedisPool) extends Lock {

  override def acquire(lockId: String, ttlSeconds: Int): Boolean = {
    var reply: String = null
    var resource: Jedis = null
    try {
      resource = this.jedisPool.getResource
      reply = resource.set(lockId, lockId, "NX", "EX", ttlSeconds)
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
      resource.del(lockId)
    }
    finally {
      resource.close()
    }
  }
}