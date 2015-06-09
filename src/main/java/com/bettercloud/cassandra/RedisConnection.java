package com.bettercloud.cassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

/**
 * Created by amit on 3/2/15.
 */
public class RedisConnection {
    protected static final Logger logger = LoggerFactory.getLogger(RedisConnection.class);
    private JedisCluster jedisConnection;

    public RedisConnection(JedisCluster jedisConnection){
        this.jedisConnection = jedisConnection;
    }

    public boolean setKey(String key, String value, int seconds){
        long val = jedisConnection.setnx(key, value);

        if (val == 1) {
            jedisConnection.expire(key,seconds);
            return true;
        }
        else return false;
    }

    public void deleteKey(String key){
        jedisConnection.del(key);
    }
}
