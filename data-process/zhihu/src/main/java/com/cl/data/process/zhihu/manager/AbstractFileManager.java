package com.cl.data.process.zhihu.manager;


import com.cl.data.process.zhihu.csv.CsvFile;
import com.cl.data.process.zhihu.exception.ServiceException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * @author yejianyu
 * @date 2019/6/26
 */
@Service
public abstract class AbstractFileManager {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    protected StringRedisTemplate redisTemplate;

    protected Long genCsvFile(String tablePrefix, CsvFile csvFile, Function<PageRequest, List<List<Object>>> getWriteData) throws Exception {
        int size = 100000;
        long maxCount;
        try {
            maxCount = count(tablePrefix);
        } catch (InvalidDataAccessResourceUsageException e) {
            throw new ServiceException("表不存在", e);
        }
        long startTime = System.currentTimeMillis();
        int page = 1;
        int count = 0;
        String filePath = csvFile.getFilePath();
        String fileName = csvFile.getFileName();
        File file = new File(filePath);
        file.mkdir();

        while (count < maxCount) {
            List<List<Object>> resultList = getWriteData.apply(PageRequest.of(page, size));
            writeToCsvFile(resultList, csvFile);
            logger.info("写入{}, page={}, 耗时{}ms", fileName, page, System.currentTimeMillis() - startTime);
            System.out.println();
            count += size;
            page++;
        }
        logger.info("写入{}/{}完成, 共耗时{}ms", filePath, fileName, System.currentTimeMillis() - startTime);
        return maxCount;
    }

    protected abstract long count(String tablePrefix);

    protected void writeToCsvFile(List<List<Object>> content, CsvFile csvFile) throws IOException {
        String filePath = csvFile.getFilePath() + File.separator + csvFile.getFileName() + CsvFile.CSV_FILE_SUFFIX;

        File file = new File(filePath);
        boolean needAppend = false;
        String[] headers = csvFile.getHeader();
        if (file.exists()) {
            try (Reader in = new FileReader(file)) {
                CSVParser csvParser = CSVFormat.DEFAULT.withHeader(headers).parse(in);
                Iterator<CSVRecord> iterator = csvParser.iterator();
                if (iterator.hasNext()) {
                    CSVRecord next = iterator.next();
                    for (int i = 0; i < headers.length; i++) {
                        if (!StringUtils.equals(next.get(i), headers[i])) {
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
                csvFormat = CSVFormat.DEFAULT.withHeader(headers);
            }
            CSVPrinter csvFilePrinter = csvFormat.print(fileWriter);
            for (List<Object> row : content) {
                csvFilePrinter.printRecord(row);
            }
        }
    }

    public void removeDuplicates(CsvFile csvFile, String setKey) {
        String path = csvFile.getFilePath();
        String fileName = csvFile.getFileName();
        String[] headers = csvFile.getHeader();
        String filePath = path + File.separator + csvFile.getFileName() + CsvFile.CSV_FILE_SUFFIX;
        File file = new File(filePath);
        int headersSize = headers.length;
        if (!file.exists()) {
            logger.warn("文件{}不存在, 无法进行去重", filePath);
            return;
        }
        logger.info("开始去重{}", filePath);
        long startTime = System.currentTimeMillis();
        try (Reader in = new FileReader(file)) {
            CSVParser csvParser = CSVFormat.DEFAULT.withHeader(headers).parse(in);
            Iterator<CSVRecord> iterator = csvParser.iterator();
            if (iterator.hasNext()) {
                CSVRecord next = iterator.next();
                for (int i = 0; i < headersSize; i++) {
                    if (!StringUtils.equals(next.get(i), headers[i])) {
                        throw new ServiceException("header不匹配， 无法去重");
                    }
                }
            }
            File newFile = new File(path + File.separator + fileName + "-unique"+ CsvFile.CSV_FILE_SUFFIX);
            try (FileWriter fileWriter = new FileWriter(newFile)) {
                CSVFormat csvFormat = CSVFormat.DEFAULT.withHeader(headers);
                CSVPrinter csvFilePrinter = csvFormat.print(fileWriter);
                int count = 0;
                int logCount = 100000;
                while (iterator.hasNext()){
                    CSVRecord next = iterator.next();
                    String id = next.get(0);
                    if (!isDuplicates(setKey, id)){
                        List<Object> row = new ArrayList<>();
                        for (String header : headers){
                            row.add(next.get(header));
                        }
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

    public void mergeFile(CsvFile csvFile, String[] fileNames) {
        if (fileNames == null || fileNames.length == 0){
            return;
        }

        String filePath = csvFile.getFilePath();
        String mergeFileName = csvFile.getFileName();
        String[] header = csvFile.getHeader();
        File newFile = new File(filePath + File.separator + mergeFileName + CsvFile.CSV_FILE_SUFFIX);

        try (FileWriter fileWriter = new FileWriter(newFile)) {
            CSVPrinter csvPrinter = CSVFormat.DEFAULT.withHeader(header).print(fileWriter);
            for (String fileName : fileNames) {
                try (Reader in = new FileReader(filePath + File.separator + fileName)) {
                    CSVParser csvParser = CSVFormat.DEFAULT.withHeader(header).withSkipHeaderRecord().parse(in);
                    csvPrinter.printRecords(csvParser);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void segmentFile(CsvFile csvFile, String[] fileNames, int segmentSize) {
        if (fileNames == null || fileNames.length == 0) {
            return;
        }
        String filePath = csvFile.getFilePath();
        String newFileName = csvFile.getFileName();
        String[] header = csvFile.getHeader();
        String segmentPath = filePath + File.separator + "segment";
        File segmentFile = new File(segmentPath);
        segmentFile.mkdir();
        int index = 0;
        int count = 0;
        FileWriter fileWriter = null;
        try {
            CSVPrinter csvPrinter = null;
            for (String fileName : fileNames) {
                try (Reader in = new FileReader(filePath + File.separator + fileName)) {
                    CSVParser csvParser = CSVFormat.DEFAULT.withHeader(header).withSkipHeaderRecord().parse(in);
                    for (CSVRecord record : csvParser) {
                        if (fileWriter == null || count >= segmentSize) {
                            if (fileWriter != null){
                                fileWriter.close();
                            }
                            count = 0;
                            File newFile = new File(segmentPath + File.separator + newFileName + index + CsvFile.CSV_FILE_SUFFIX);
                            index++;
                            fileWriter = new FileWriter(newFile);
                            csvPrinter = CSVFormat.DEFAULT.withHeader(header).print(fileWriter);
                        }
                        csvPrinter.printRecord(record);
                        count++;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new ServiceException(e);
                }
            }
        } finally {
            if (fileWriter != null){
                try {
                    fileWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private Boolean isDuplicates(String setKey, String id) {

        Long count = redisTemplate.opsForSet().add(setKey, setKey);
        return count != null && count <= 0;
    }
}
