package com.cl.data.process.service;

import com.cl.data.process.entity.Topic;
import com.cl.data.process.exception.ServiceException;
import com.cl.data.process.manager.RedisManager;
import com.cl.data.process.repository.TopicRepository;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author yejianyu
 * @date 2019/7/8
 */
@Service
public class TopicService {

    private Logger logger = LoggerFactory.getLogger(TopicService.class);

    private final TopicRepository topicRepository;

    private final ThreadPoolTaskExecutor taskExecutor;

    private final RedisManager redisManager;

    private static final Pattern NUMBER_SIGN_PATTERN = Pattern.compile("^#+|#+$");
    private static final String TOPIC_CATEGORY_LIST = "榜单";
    private static final String TABLE_NAME_TOPIC = "topic";

    public TopicService(TopicRepository topicRepository, RedisManager redisManager, ThreadPoolTaskExecutor taskExecutor) {
        this.topicRepository = topicRepository;
        this.redisManager = redisManager;
        this.taskExecutor = taskExecutor;
    }

    public List<Topic> listNotListCategoryTopic(int page, int size) {
        return topicRepository.findAllByCategoryNot(TOPIC_CATEGORY_LIST, PageRequest.of(page, size)).getContent();
    }

    public long classifyTopicToFile(String resultPath, String tableName) throws ServiceException {
        int size = 100000;
        long maxCount;
        try {
            maxCount = topicRepository.countAllByCategoryNot(TOPIC_CATEGORY_LIST);
        } catch (InvalidDataAccessResourceUsageException e) {
            throw new ServiceException("表 " + tableName + " 不存在", e);
        }
        long startTime = System.currentTimeMillis();
        int page = 0;
        int count = 0;
        File resultFile = new File(resultPath);
        resultFile.mkdirs();
        while (count < maxCount) {
            List<Topic> topicList = listNotListCategoryTopic(page, size);
            Map<String, Set<String>> map = classifyTopic(topicList);
            CountDownLatch countDownLatch = new CountDownLatch(map.size());
            map.forEach((s, set) -> {
                taskExecutor.execute(() -> {
                    try {
                        writeToCsvFile(set, resultPath, s);
                    }finally {
                        countDownLatch.countDown();
                    }
                });
            });
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                throw new ServiceException(e);
            }
            count += size;
            page++;
            logger.debug("表{}完成进度{}/{}, 已耗时{}ms", tableName, count ,maxCount, System.currentTimeMillis() - startTime);
        }
        logger.info("话题表{}分类完成, 结果在{}, 共耗时{}ms", tableName, resultPath, System.currentTimeMillis() - startTime);
        return count;
    }

    private Map<String, Set<String>> classifyTopic(List<Topic> topicList) {
        Map<String, Set<String>> map = new HashMap<>(16);
        for (Topic topic : topicList) {
            String category = topic.getCategory();
            Set<String> set;
            if (map.containsKey(category)) {
                set = map.get(category);
            } else {
                set = new HashSet<>();
                map.put(category, set);
            }
            Matcher matcher = NUMBER_SIGN_PATTERN.matcher(topic.getCardTypeName().trim());
            String cardTypeName = matcher.replaceAll("");
            set.add(cardTypeName.toLowerCase());
        }
        return map;
    }

    private void writeToCsvFile(Set<String> topicSet, String path, String topicCategory) throws ServiceException {
        String filePath = path + File.separator + topicCategory + ".csv";

        File file = new File(filePath);

        try (Writer writer = new OutputStreamWriter(new FileOutputStream(file, true), StandardCharsets.UTF_8)) {
            CSVPrinter csvFilePrinter = CSVFormat.DEFAULT.print(writer);
            for (String topic : topicSet) {
                if (!redisManager.isDuplicates(TABLE_NAME_TOPIC + "_" + topicCategory, topic)){
                    csvFilePrinter.printRecord("#" + topic + "#");
                }
            }
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

}
