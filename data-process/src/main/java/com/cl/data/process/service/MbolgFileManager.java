package com.cl.data.process.service;

import com.cl.data.process.entity.MblogFromId;
import com.cl.data.process.exception.ServiceException;
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
import java.util.*;

/**
 * @author yejianyu
 * @date 2019/6/26
 */
@Service
public class MbolgFileManager {

    private Logger logger = LoggerFactory.getLogger(MbolgFileManager.class);

    private static final String[] HEADER = new String[]{"mid", "uid", "text"};
    private static final String MBOLG_FROM_UID_MID_KEY = "mbolg_from_uid_mid_key";
    private static final String MID_KEY_PREFIX = "mbolg_from_uid_";

    private final MblogServiceImpl mblogService;

    private final StringRedisTemplate redisTemplate;

    @Autowired
    public MbolgFileManager(MblogServiceImpl mblogService, StringRedisTemplate redisTemplate) {
        this.mblogService = mblogService;
        this.redisTemplate = redisTemplate;
    }

    public Long genCsvFile(String filePath, String fileName) throws Exception {
        int size = 100000;
        long maxCount;
        try {
            maxCount = mblogService.count();
        } catch (InvalidDataAccessResourceUsageException e) {
            throw new ServiceException("表 " + fileName + " 不存在", e);
        }
        long startTime = System.currentTimeMillis();
        int page = 0;
        int count = 0;
        while (count < maxCount) {
            List<MblogFromId> mblogList = mblogService.findAll(page, size);
            writeToCsvFile(mblogList, filePath, fileName);
            logger.debug("写入page={}, 耗时{}ms", page, System.currentTimeMillis() - startTime);
            System.out.println();
            count += size;
            page++;
        }
        logger.info("写入{}/{}完成, 共耗时{}ms", filePath, fileName, System.currentTimeMillis() - startTime);
        return maxCount;
    }

    private void writeToCsvFile(List<MblogFromId> content, String path, String fileName) throws IOException {
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
            for (MblogFromId mblog : content) {
                List<Object> row = new ArrayList<>();
                row.add(mblog.getMid());
                row.add(mblog.getUid());
                row.add(mblog.getText());
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
                    String mid = next.get("mid");
                    if (!isDuplicates(mid)){
                        List<Object> row = new ArrayList<>();
                        row.add(next.get("mid"));
                        row.add(next.get("uid"));
                        row.add(next.get("text"));
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

    private Boolean isDuplicates(String mid) {

        Long count = redisTemplate.opsForSet().add(MBOLG_FROM_UID_MID_KEY, mid);
        return count != null && count <= 0;
//        Boolean isAbsent = redisTemplate.opsForValue().setIfAbsent(MID_KEY_PREFIX + mid, mid);
//        return isAbsent != null && !isAbsent;
    }
}
