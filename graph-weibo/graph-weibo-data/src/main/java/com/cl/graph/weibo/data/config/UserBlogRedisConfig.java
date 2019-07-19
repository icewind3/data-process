package com.cl.graph.weibo.data.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;

/**
 * @author yejianyu
 * @date 2019/7/15
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
        LettucePoolingClientConfiguration clientConfiguration = LettucePoolingClientConfiguration.builder()
                .commandTimeout(Duration.ofSeconds(5000L))
                .build();
        return new LettuceConnectionFactory(redisConfiguration, clientConfiguration);
    }

    @Bean(name = "userBlogRedisTemplate")
    public StringRedisTemplate userBlogRedisTemplate() {
        return new StringRedisTemplate(userBlogConnectionFactory());
    }
}
