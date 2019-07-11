package com.cl.data.process.manager;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * @author yejianyu
 * @date 2019/7/8
 */
@Component
public class RedisManager {

    private final StringRedisTemplate redisTemplate;

    public RedisManager(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public Boolean isDuplicates(String setKey, String val) {
        Long count = redisTemplate.opsForSet().add(setKey, val);
        return count != null && count <= 0;
    }
}
