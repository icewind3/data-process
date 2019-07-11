package com.cl.data.process.manager;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author yejianyu
 * @date 2019/7/8
 */
@Component
public class UserFileManager {

    private static final String RESULT_DIC_PREFIX = "result_";
    private static final String REDIS_HASH_BLOG_LABEL_KEY = "blog_label";
    private final Logger logger = LoggerFactory.getLogger(UserFileManager.class);

    private final ThreadPoolTaskExecutor taskExecutor;

    @Resource(name = "userBlogRedisTemplate")
    private StringRedisTemplate userBlogRedisTemplate;

    @Resource(name = "blogLabelRedisTemplate")
    private StringRedisTemplate blogLabelRedisTemplate;

    public UserFileManager(ThreadPoolTaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    public void batchAddMidToUser(String dicPath) {
        File dicFile = new File(dicPath);
        if (dicFile.isDirectory()) {
            String[] fileNames = dicFile.list((dir, name) -> StringUtils.endsWith(name, ".csv"));
            if (fileNames == null || fileNames.length == 0) {
                return;
            }
            CountDownLatch countDownLatch = new CountDownLatch(fileNames.length);
            for (String fileName : fileNames) {
                taskExecutor.execute(() -> {
                    try {
                        addMidToUser(dicPath + File.separator + fileName);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            addMidToUser(dicPath);
        }
    }

    public void batchAddLabelToUser(String dicPath, String labelType) {
        File dicFile = new File(dicPath);
        if (dicFile.isDirectory()) {
            String[] fileNames = dicFile.list((dir, name) -> StringUtils.endsWith(name, ".csv"));
            if (fileNames == null || fileNames.length == 0) {
                return;
            }
            CountDownLatch countDownLatch = new CountDownLatch(fileNames.length);
            for (String fileName : fileNames) {
                taskExecutor.execute(() -> {
                    try {
                        addLabelToUser(dicPath + File.separator + fileName, labelType);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            addLabelToUser(dicPath, labelType);
        }
    }

    public void addLabelToUser(String filePath, String labelType) {
        File file = new File(filePath);
        if (file.isDirectory()) {
            return;
        }
        long startTime = System.currentTimeMillis();
        if (logger.isInfoEnabled()) {
            logger.info("开始处理文件{}", filePath);
        }
        try (CSVParser csvParser = CSVParser.parse(file, StandardCharsets.UTF_8, CSVFormat.DEFAULT)) {
            File resultFile = new File(file.getParent() + File.separator + RESULT_DIC_PREFIX + labelType);
            resultFile.mkdir();
            File newFile = new File(resultFile.getPath() + File.separator + file.getName());
            String[] header = new String[]{"uid", "label", "count"};
            AtomicInteger userCount = new AtomicInteger();
            try (CSVPrinter csvPrinter = CSVFormat.DEFAULT.withHeader(header).print(newFile, StandardCharsets.UTF_8)) {
                int oneTaskSize = 200;
                ReentrantLock writeLock = new ReentrantLock();
                Iterator<CSVRecord> recordIterator = csvParser.iterator();
                int count = 0;
                List<List<String>> taskList = new ArrayList<>();
                List<String> uidList = new ArrayList<>();
                while (recordIterator.hasNext()) {
                    CSVRecord record = recordIterator.next();
                    String uid = record.get(0);
                    uidList.add(uid);
                    count++;
                    if (count % oneTaskSize == 0) {
                        taskList.add(uidList);
                        uidList = new ArrayList<>();
                    }
                }
                CountDownLatch countDownLatch = new CountDownLatch(taskList.size());
                taskList.forEach(idList -> {
                    taskExecutor.execute(() -> {
                        for (String uid : idList) {
                            Map<String, Integer> map = new HashMap<>(16);
                            Set<Object> midSet = getUserBlogByUid(uid);
                            midSet.forEach(mid -> {
                                getBlogLabelByMid((String) mid, labelType).forEach(s -> {
                                    map.merge(s, 1, Integer::sum);
                                });
                            });
                            writeLock.lock();
                            map.forEach((s, integer) -> {
                                try {
                                    csvPrinter.printRecord(uid, s, integer);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            });
                            writeLock.unlock();
                            userCount.getAndIncrement();
                            if (logger.isInfoEnabled() && userCount.get() % 5000 == 0) {
                                logger.info("文件{},已处理用户{}条, 已耗时{}ms", filePath, userCount,
                                        System.currentTimeMillis() - startTime);
                            }
                        }
                        countDownLatch.countDown();
                    });
                });
                try {
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (logger.isInfoEnabled()) {
            logger.info("已完成文件{}的处理，共耗时{}ms", filePath, System.currentTimeMillis() - startTime);
        }
    }

    public void addMidToUser(String filePath) {
        File file = new File(filePath);
        if (file.isDirectory()) {
            return;
        }
        long startTime = System.currentTimeMillis();
        if (logger.isInfoEnabled()) {
            logger.info("添加mid, 开始处理文件{}", filePath);
        }
        try (CSVParser csvParser = CSVParser.parse(file, StandardCharsets.UTF_8, CSVFormat.DEFAULT)) {
            File resultFile = new File(file.getParent() + File.separator + RESULT_DIC_PREFIX + "mid");
            resultFile.mkdir();
            File newFile = new File(resultFile.getPath() + File.separator + file.getName());
            String[] header = new String[]{"uid", "mid"};
            int userCount = 0;
            try (CSVPrinter csvPrinter = CSVFormat.DEFAULT.withHeader(header).print(newFile, StandardCharsets.UTF_8)) {
                for (CSVRecord record : csvParser) {
                    userCount++;
                    String uid = record.get(0);
                    getUserBlogByUid(uid).forEach(mid -> {
                        try {
                            csvPrinter.printRecord(uid, mid);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                    if (userCount % 100000 == 0) {
                        csvPrinter.flush();
                        if (logger.isInfoEnabled()) {
                            logger.info("添加mid, 文件{},已处理用户{}条, 已耗时{}ms", filePath, userCount,
                                    System.currentTimeMillis() - startTime);
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (logger.isInfoEnabled()) {
            logger.info("添加mid, 已完成文件{}的处理，共耗时{}ms", filePath, System.currentTimeMillis() - startTime);
        }
    }

    public Set<Object> getUserBlogByUid(String uid) {
        return userBlogRedisTemplate.opsForHash().keys(uid);
    }

    public List<String> getBlogLabelByMid(Set<Object> midSet, String labelType) {
        List<Object> list = blogLabelRedisTemplate.opsForHash().multiGet(REDIS_HASH_BLOG_LABEL_KEY, midSet);
        List<String> labelList = new ArrayList<>();
        list.forEach(json -> {
            if (json == null){
                return;
            }
            try {
                JSONObject jsonObject = JSON.parseObject((String) json);
                JSONArray array = (JSONArray) jsonObject.get(labelType);
                if (array != null) {
                    labelList.addAll(array.toJavaList(String.class));
                }
            } catch (JSONException ignored) {
            }
        });
        return labelList;
    }

    public List<String> getBlogLabelByMid(String mid, String labelType) {
        String jsonString = (String) blogLabelRedisTemplate.opsForHash().get(REDIS_HASH_BLOG_LABEL_KEY, mid);
        if (jsonString != null) {
            try {
                JSONObject jsonObject = JSON.parseObject(jsonString);
                JSONArray array = (JSONArray) jsonObject.get(labelType);
                if (array == null) {
                    return Collections.emptyList();
                }
                return array.toJavaList(String.class);
            } catch (JSONException e) {
//                logger.warn("String转json出错，String = {}", jsonString);
            }
        }
        return Collections.emptyList();
    }


}
