package com.cl.data.process.manager;

import com.cl.data.process.exception.ServiceException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * @author yejianyu
 * @date 2019/7/10
 */
@Component
public class UserBlogFileFilterManager {

    private final Logger logger = LoggerFactory.getLogger(UserBlogFileFilterManager.class);

    private final ThreadPoolTaskExecutor taskExecutor;

    public UserBlogFileFilterManager(ThreadPoolTaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    public void filterFileByOneKey(String keyFilePath, String oriFilePath, int filterIndex, String resultPath,
                                   boolean needInclude) throws ServiceException {
        Set<String> keySet = genFilterSet(keyFilePath);
        File oriFile = new File(oriFilePath);
        if (oriFile.isDirectory()) {
            String[] files = oriFile.list();
            if (files == null || files.length == 0) {
                throw new ServiceException("文件夹" + oriFilePath + "下无文件，无法过滤点文件");
            }
            CountDownLatch countDownLatch = new CountDownLatch(files.length);
            for (String file : files) {
                taskExecutor.execute(() -> {
                    try {
                    filterFileByOneKey(keySet, oriFilePath + File.separator + file, filterIndex, resultPath,
                            needInclude);
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
            filterFileByOneKey(keySet, oriFilePath, filterIndex, resultPath, needInclude);
        }
    }

    private void filterFileByOneKey(Set<String> filterSet, String oriFilePath, int filterIndex, String resultPath,
                                    boolean needInclude) throws ServiceException {
        File oriFile = new File(oriFilePath);
        if (oriFile.isDirectory()) {
            return;
        }
        long startTime = System.currentTimeMillis();
        try (CSVParser csvParser = CSVParser.parse(oriFile, StandardCharsets.UTF_8, CSVFormat.DEFAULT)){
            Iterator<CSVRecord> readIterator = csvParser.iterator();
            File resultDir = new File(resultPath);
            resultDir.mkdir();
            File newFile = new File(resultPath + File.separator + oriFile.getName());
            try (CSVPrinter csvFilePrinter = CSVFormat.DEFAULT.print(newFile, StandardCharsets.UTF_8)) {
                while (readIterator.hasNext()) {
                    CSVRecord record = readIterator.next();
                    String key = record.get(filterIndex);
                    boolean needWrite = (needInclude && filterSet.contains(key))
                            || (!needInclude && !filterSet.contains(key));
                    if (needWrite) {
                        csvFilePrinter.printRecord(record);
                    }
                }
            }
            logger.info("生成点文件{}完成, 共耗时{}ms", newFile.getPath(), System.currentTimeMillis() - startTime);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    private Set<String> genFilterSet(String keyFilePath) throws ServiceException {
        Set<String> keySet = new HashSet<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(keyFilePath), StandardCharsets.UTF_8))) {
            String str;
            while ((str = br.readLine()) != null) {
                keySet.add(str);
            }
        } catch (IOException e) {
            throw new ServiceException(e);
        }
        return keySet;
    }

}
