package com.cl.graph.weibo.data.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author yejianyu
 * @date 2019/7/18
 */
@Configuration
public class ThreadPoolConfig {

    @Bean(name = "followingThreadPoolTaskExecutor")
    public ThreadPoolTaskExecutor followingThreadPoolTaskExecutor() {
        return threadPoolTaskExecutor("following-");
    }

    @Bean(name = "commonThreadPoolTaskExecutor")
    public ThreadPoolTaskExecutor threadPoolTaskExecutor() {
        return threadPoolTaskExecutor("weibo-");
    }

    private ThreadPoolTaskExecutor threadPoolTaskExecutor(String threadNamePrefix) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(200);
        executor.setQueueCapacity(10000);
        executor.setKeepAliveSeconds(60);
        executor.setThreadNamePrefix(threadNamePrefix);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        return executor;
    }
}
