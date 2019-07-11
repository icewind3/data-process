package com.cl.data.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.cl.data.process.manager.UserFileManager;
import com.cl.data.process.repository.TopicRepository;
import com.cl.data.process.service.TopicService;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


@RunWith(SpringRunner.class)
@SpringBootTest
public class DataProcessApplicationTests {

    private Logger logger = LoggerFactory.getLogger(DataProcessApplicationTests.class);

    @Autowired
    private ThreadPoolTaskExecutor taskExecutor;

    @Autowired
    private UserFileManager userFileManager;

    @Test
    public void contextLoads() {
        AtomicInteger count = new AtomicInteger();
        Phaser phaser = new Phaser();
        phaser.register();
        for (int i = 0; i < 1000; i++) {
//            CompletableFuture.supplyAsync(() -> {
//                Random random = new Random();
//                int second = random.nextInt(10);
//                try {
//                    Thread.sleep(second * 1000);
//                    count.getAndIncrement();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }, taskExecutor);
//            list.stream().map(a -> ).collect(Collectors.toList()).stream().map(CompletableFuture::join).collect(Collectors.toList());
            phaser.register();
            taskExecutor.execute(() -> {
                Random random = new Random();
                int second = random.nextInt(10);
                try {
                    Thread.sleep(second * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("结束" + count.getAndIncrement());
                phaser.arriveAndDeregister();
            });
        }
        phaser.arriveAndDeregister();
        System.out.println("结束" + count.get());
        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void test() throws IOException {
        String filePath = "C:/Users/cl32/Downloads/user/part/weibo_followers__10_w.csv";
        File file = new File(filePath);
        long startTime = System.currentTimeMillis();
        if (logger.isInfoEnabled()) {
            logger.info("开始处理文件{}", filePath);
        }
        long searchMidTime = 0L;
        long searchLabelTime = 0L;
        int midSize = 0;
        int oneTaskSize = 100;
        try (CSVParser csvParser = CSVParser.parse(file, StandardCharsets.UTF_8, CSVFormat.DEFAULT)) {
            Iterator<CSVRecord> recordIterator = csvParser.iterator();
            int count = 0;
            List<String> uidList = new ArrayList<>();
            while (recordIterator.hasNext()) {
                CSVRecord record = recordIterator.next();
                String uid = record.get(0);
                uidList.add(uid);
                count++;
                if (count % oneTaskSize == 0) {
                    break;
                }
            }

            for (String uid : uidList) {
                Map<String, Integer> map = new HashMap<>(16);
                long firstTime = System.currentTimeMillis();
                Set<Object> midSet = userFileManager.getUserBlogByUid(uid);
                long secondTime = System.currentTimeMillis();
                searchMidTime += secondTime - firstTime;
                midSet.forEach(mid -> {
                    userFileManager.getBlogLabelByMid((String) mid, "product");
                });
                int size = midSet.size();
                midSize += size;
                if (size > 0) {
                    long thirdTime = System.currentTimeMillis() - secondTime;
                    searchLabelTime += thirdTime;
//                    logger.info("查找标签，midSet size = {}, 耗时{} ms, 平均耗时{} ms", midSet.size(), thirdTime, thirdTime / size);
                }
            }
        }
        logger.info("测试{}个id, 总耗时{} ms, 查询mid={}ms, 平均{}ms，查询label= {}ms, midSize={}, 平均{}ms",
                oneTaskSize, System.currentTimeMillis() - startTime, searchMidTime, searchMidTime / 100, searchLabelTime, searchLabelTime/midSize, midSize);
    }


}
