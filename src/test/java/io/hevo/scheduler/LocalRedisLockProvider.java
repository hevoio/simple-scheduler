package io.hevo.scheduler;

import io.hevo.scheduler.lock.RedisBasedLock;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class LocalRedisLockProvider {

    public static RedisBasedLock createLock() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(25);
        poolConfig.setMaxIdle(25);
        poolConfig.setMaxWaitMillis(10_000);

        return new RedisBasedLock(new JedisPool(poolConfig, "localhost", 6379));
    }
}
