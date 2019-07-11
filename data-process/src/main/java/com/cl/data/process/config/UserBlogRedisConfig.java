package com.cl.data.process.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConfiguration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * @author yejianyu
 * @date 2019/6/26
 */
@Configuration
public class UserBlogRedisConfig {

    @Value("${spring.redis.user-blog.host}")
    private String host;

    @Value("${spring.redis.user-blog.port}")
    private Integer port;

    @Bean
    public RedisConnectionFactory userBlogConnectionFactory(){
        RedisStandaloneConfiguration redisConfiguration = new RedisStandaloneConfiguration(host, port);
        return new LettuceConnectionFactory(redisConfiguration);

    }

    @Bean(name = "userBlogRedisTemplate")
    public StringRedisTemplate userBlogRedisTemplate() {
        return new StringRedisTemplate(userBlogConnectionFactory());
    }
}
