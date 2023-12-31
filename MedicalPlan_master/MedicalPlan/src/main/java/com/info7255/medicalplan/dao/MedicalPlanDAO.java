package com.info7255.medicalplan.dao;

import org.springframework.stereotype.Repository;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPooled;

import java.util.Map;
import java.util.Set;

import static com.info7255.medicalplan.Constants.Constants.PORT;
import static com.info7255.medicalplan.Constants.Constants.REDIS_URL;


@Repository
public class MedicalPlanDAO {
    private final JedisPooled jedis = new JedisPooled(REDIS_URL, PORT);

    public void sadd(String key, String value) {
        jedis.sadd(key, value);
    }

    public Set<String> sMembers(String key) {
        return jedis.smembers(key);
    }

    public void hSet(String key, String field, String value) {
        jedis.hset(key, field, value);
    }

    public String hGet(String key, String field) {
        return jedis.hget(key, field);
    }

    public Map<String, String> hGetAll(String key) {
        return jedis.hgetAll(key);
    }

    public void lpush(String key, String value) {
        jedis.lpush(key, value);
    }

    public boolean existsKey(String key) {
        return jedis.exists(key);
    }

    public Set<String> getKeysByPattern(String pattern) {
        return jedis.keys(pattern);
    }

    public long deleteKeys(String[] keys) {
        return jedis.del(keys);
    }

    public Map<String,String> getAllValuesByKey(String key) {
            return jedis.hgetAll(key);

    }
    public void addSetValue(String key, String value) {
            jedis.sadd(key, value);
    }

    public Set<String> getKeys(String pattern){
            return jedis.keys(pattern);

    }

    public Set<String> getAllMembers(String key) {
        try (Jedis jedis = new Jedis("localhost")) {
            return jedis.smembers(key);
        }
    }
}
