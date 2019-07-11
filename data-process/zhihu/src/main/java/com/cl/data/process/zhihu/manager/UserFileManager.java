package com.cl.data.process.zhihu.manager;


import com.cl.data.process.zhihu.entity.User;
import com.cl.data.process.zhihu.exception.ServiceException;
import com.cl.data.process.zhihu.mapper.UserMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author yejianyu
 * @date 2019/6/26
 */
@Service
public class UserFileManager {

    private Logger logger = LoggerFactory.getLogger(UserFileManager.class);

    private static final String[] HEADER = new String[]{"id", "gender","voteupCount", "thankedCount"};
    private static final String USER_KEY = "user_key";

    private final UserMapper userMapper;

    private final StringRedisTemplate redisTemplate;

    @Autowired
    public UserFileManager(UserMapper userMapper, StringRedisTemplate redisTemplate) {
        this.userMapper = userMapper;
        this.redisTemplate = redisTemplate;
    }

    public Long genCsvFile(String tablePrefix, String filePath, String fileName) throws Exception {
        int size = 100000;
        long maxCount;
        try {
            maxCount = userMapper.count(tablePrefix);
        } catch (InvalidDataAccessResourceUsageException e) {
            throw new ServiceException("表 " + fileName + " 不存在", e);
        }
        long startTime = System.currentTimeMillis();
        int page = 1;
        int count = 0;
        while (count < maxCount) {
            List<User> userList = userMapper.findAll(tablePrefix, page, size);
            writeToCsvFile(userList, filePath, fileName);
            logger.info("写入page={}, 耗时{}ms", page, System.currentTimeMillis() - startTime);
            System.out.println();
            count += size;
            page++;
        }
        logger.info("写入{}/{}完成, 共耗时{}ms", filePath, fileName, System.currentTimeMillis() - startTime);
        return maxCount;
    }

    private void writeToCsvFile(List<User> content, String path, String fileName) throws IOException {
        String filePath = path + File.separator + fileName + ".csv";

        File file = new File(filePath);
        boolean needAppend = false;
        if (file.exists()) {
            try (Reader in = new FileReader(file)) {
                CSVParser csvParser = CSVFormat.DEFAULT.withHeader(HEADER).parse(in);
                Iterator<CSVRecord> iterator = csvParser.iterator();
                if (iterator.hasNext()) {
                    CSVRecord next = iterator.next();
                    for (int i = 0; i < HEADER.length; i++) {
                        if (!StringUtils.equals(next.get(i), HEADER[i])) {
                            throw new ServiceException("header不匹配， 无法新增记录");
                        }
                    }
                }
            }
            needAppend = true;
        }

        try (FileWriter fileWriter = new FileWriter(file, needAppend)) {
            CSVFormat csvFormat;
            if (needAppend) {
                csvFormat = CSVFormat.DEFAULT;
            } else {
                csvFormat = CSVFormat.DEFAULT.withHeader(HEADER);
            }
            CSVPrinter csvFilePrinter = csvFormat.print(fileWriter);
            for (User user : content) {
                List<Object> row = new ArrayList<>();
                row.add(user.getId());
                row.add(user.getGender());
                row.add(user.getVoteupCount());
                row.add(user.getThankedCount());
                csvFilePrinter.printRecord(row);
            }
        }
    }

    public void removeDuplicates(String path, String fileName) {
        String filePath = path + File.separator + fileName + ".csv";
        File file = new File(filePath);
        if (!file.exists()) {
            logger.warn("文件{}不存在, 无法进行去重", filePath);
            return;
        }
        logger.info("开始去重{}", filePath);
        long startTime = System.currentTimeMillis();
        try (Reader in = new FileReader(file)) {
            CSVParser csvParser = CSVFormat.DEFAULT.withHeader(HEADER).parse(in);
            Iterator<CSVRecord> iterator = csvParser.iterator();
            if (iterator.hasNext()) {
                CSVRecord next = iterator.next();
                for (int i = 0; i < HEADER.length; i++) {
                    if (!StringUtils.equals(next.get(i), HEADER[i])) {
                        throw new ServiceException("header不匹配， 无法去重");
                    }
                }
            }
            File newFile = new File(path + File.separator + fileName + "-unique"+ ".csv");
            try (FileWriter fileWriter = new FileWriter(newFile)) {
                CSVFormat csvFormat = CSVFormat.DEFAULT.withHeader(HEADER);
                CSVPrinter csvFilePrinter = csvFormat.print(fileWriter);
                int count = 0;
                int logCount = 100000;
                while (iterator.hasNext()){
                    CSVRecord next = iterator.next();
                    String id = next.get("id");
                    if (!isDuplicates(id)){
                        List<Object> row = new ArrayList<>();
                        row.add(next.get("id"));
                        row.add(next.get("gender"));
                        row.add(next.get("voteupCount"));
                        row.add(next.get("thankedCount"));
                        csvFilePrinter.printRecord(row);
                    }

                    if (++count >= logCount) {
                        logger.info("去重{}已完成{}, 总共已耗时{}ms", filePath, count, System.currentTimeMillis() - startTime);
                        logCount += 100000;
                    }
               }
            }
            logger.info("去重{}完成, 共耗时{}ms", filePath, System.currentTimeMillis() - startTime);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private Boolean isDuplicates(String id) {

        Long count = redisTemplate.opsForSet().add(USER_KEY, id);
        return count != null && count <= 0;
    }
}
