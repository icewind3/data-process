package com.cl.file.process.service;

import com.cl.data.process.core.exception.ServiceException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author yejianyu
 * @date 2019/7/5
 */
@Service
public class FileFilterService {

    private Logger logger = LoggerFactory.getLogger(FileFilterService.class);

    public void filterFileByOneKey(String keyFilePath, String oriFilePath, int filterIndex, String resultPath,
                                   boolean needInclude) throws ServiceException {
        Set<String> keySet = genFilterSet(keyFilePath);
        File oriFile = new File(oriFilePath);
        if (oriFile.isDirectory()) {
            String[] files = oriFile.list();
            if (files == null || files.length == 0) {
                throw new ServiceException("文件夹" + oriFilePath + "下无文件，无法过滤点文件");
            }
            for (String file : files) {
                filterFileByOneKey(keySet, oriFilePath + File.separator + file, filterIndex, resultPath,
                        needInclude);
            }
        } else {
            filterFileByOneKey(keySet, oriFilePath, filterIndex, resultPath, needInclude);
        }
    }

    public void filterEdgeFile(String keyFilePath, String oriFilePath, int fromIndex, int toIndex, String resultPath)
            throws ServiceException {
        Set<String> keySet = genFilterSet(keyFilePath);
        File oriFile = new File(oriFilePath);
        if (oriFile.isDirectory()) {
            String[] files = oriFile.list();
            if (files == null || files.length == 0) {
                throw new ServiceException("文件夹" + oriFilePath + "下无文件，无法过滤边文件");
            }
            for (String file : files) {
                filterEdgeFile(keySet, oriFilePath + File.separator + file, fromIndex, toIndex, resultPath);
            }
        } else {
            filterEdgeFile(keySet, oriFilePath, fromIndex, toIndex, resultPath);
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
            String[] header = {};
            if (readIterator.hasNext()) {
                CSVRecord headerRecord = readIterator.next();
                header = extractHeader(headerRecord);
            }
            File resultDir = new File(resultPath);
            resultDir.mkdir();
            File newFile = new File(resultPath + File.separator + oriFile.getName());
            try (CSVPrinter csvFilePrinter = CSVFormat.DEFAULT.withHeader(header).print(newFile, StandardCharsets.UTF_8)) {
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


    private void filterEdgeFile(Set<String> filterSet, String oriFilePath, int fromIndex, int toIndex, String resultPath)
            throws ServiceException {
        long startTime = System.currentTimeMillis();
        File oriFile = new File(oriFilePath);
        try (Reader in = new InputStreamReader(new FileInputStream(oriFile), StandardCharsets.UTF_8)) {
            CSVParser csvParser = CSVFormat.DEFAULT.parse(in);
            Iterator<CSVRecord> readIterator = csvParser.iterator();
            String[] header = {};
            if (readIterator.hasNext()) {
                CSVRecord headerRecord = readIterator.next();
                header = extractHeader(headerRecord);
            }
            File resultDir = new File(resultPath);
            resultDir.mkdir();
            File newFile = new File(resultPath + File.separator + oriFile.getName());
            try (Writer writer = new OutputStreamWriter(new FileOutputStream(newFile), StandardCharsets.UTF_8)) {
                CSVPrinter csvFilePrinter = CSVFormat.DEFAULT.withHeader(header).print(writer);
                while (readIterator.hasNext()) {
                    CSVRecord record = readIterator.next();
                    String fromKey = record.get(fromIndex);
                    String toKey = record.get(toIndex);
                    if (filterSet.contains(fromKey) && filterSet.contains(toKey)) {
                        csvFilePrinter.printRecord(record);
                    }
                }
            }
            logger.info("生成边文件{}完成, 共耗时{}ms", newFile.getPath(), System.currentTimeMillis() - startTime);
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

    private String[] extractHeader(CSVRecord headerRecord) {
        int headerSize = headerRecord.size();
        String[] header = new String[headerSize];
        for (int i = 0; i < headerSize; i++) {
            header[i] = headerRecord.get(i);
        }
        return header;
    }
}
