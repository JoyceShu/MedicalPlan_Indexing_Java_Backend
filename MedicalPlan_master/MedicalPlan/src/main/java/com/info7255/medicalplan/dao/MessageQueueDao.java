package com.info7255.medicalplan.dao;

import org.springframework.stereotype.Repository;
import redis.clients.jedis.Jedis;

@Repository
public class MessageQueueDao {


    public void addToQueue(String queue, String value) {
        try (Jedis jedis = new Jedis("localhost")) {
            jedis.lpush(queue, value);
        }
    }
}
