package com.bettercloud.cassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by amit on 3/2/15.
 */
public class RedisConnection {
    protected static final Logger logger = LoggerFactory.getLogger(RedisConnection.class);
    private JedisCluster jedisConnection;
    private JedisPoolConfig jedisPoolConfig;
    private JedisPool jedisPool;

    public void init(JedisCluster jedis){
        this.jedisConnection = jedis;
    }

    public String getKey(String key){
        return jedisConnection.get(key);
    }

    public boolean setKey(String key, String value){
        long val = jedisConnection.setnx(key, value);
        logger.info(val + "-" + key);
        if (val == 1) {
            return true;
        }
                else return false;
    }

    public boolean setKey(String key, String value, int seconds){
        long val = jedisConnection.setnx(key, value);
        logger.info(val + "-" + key);
        if (val == 1) {
            jedisConnection.expire(key,seconds);
            return true;
        }
        else return false;
    }

    public boolean existsKey(String key){
        return jedisConnection.exists(key);
    }

    public void closeConnection(){
        BetterCloud.getInstance().returnRedisResource(jedisConnection);
    }

    public boolean isConnected(){
        return true;
    }

    public void expire(String key, int seconds){
        jedisConnection.expire(key, seconds);
    }
}
