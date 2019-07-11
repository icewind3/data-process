package com.cl.data.process.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * @author yejianyu
 * @date 2019/6/26
 */
@Configuration
public class BlogLabelRedisConfig {

    @Value("${spring.redis.blog-label.host}")
    private String host;

    @Value("${spring.redis.blog-label.port}")
    private Integer port;

    @Bean
    public RedisConnectionFactory blogLabelConnectionFactory(){
        RedisStandaloneConfiguration redisConfiguration = new RedisStandaloneConfiguration(host, port);
        return new LettuceConnectionFactory(redisConfiguration);

    }

    @Bean(name = "blogLabelRedisTemplate")
    public StringRedisTemplate blogLabelRedisTemplate() {
        return new StringRedisTemplate(blogLabelConnectionFactory());
    }
}
