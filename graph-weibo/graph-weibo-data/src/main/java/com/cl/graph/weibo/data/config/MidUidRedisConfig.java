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
public class MidUidRedisConfig {

    @Value("${spring.redis.mid-uid.host}")
    private String host;

    @Value("${spring.redis.mid-uid.port}")
    private Integer port;

    @Bean
    public RedisConnectionFactory midUidConnectionFactory(){
        RedisStandaloneConfiguration redisConfiguration = new RedisStandaloneConfiguration(host, port);
        LettucePoolingClientConfiguration clientConfiguration = LettucePoolingClientConfiguration.builder()
                .commandTimeout(Duration.ofSeconds(5000L))
                .build();
        return new LettuceConnectionFactory(redisConfiguration, clientConfiguration);
    }

    @Bean(name = "midUidRedisTemplate")
    public StringRedisTemplate midUidRedisTemplate() {
        return new StringRedisTemplate(midUidConnectionFactory());
    }
}
