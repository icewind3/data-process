package com.cl.file.process.service;

import com.cl.data.process.core.constant.CommonConstants;
import com.cl.data.process.core.constant.FileSuffixConstants;
import com.cl.data.process.core.exception.ServiceException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * @author yejianyu
 * @date 2019/7/4
 */
@Service
public class WordProcessService {

    private static final String RESULT_FILE_NAME =  "RESULT";

    /**
     *
     *  将原文件目录下的所有txt文件里的词进行词频统计
     * @param wordFilePath 需统计词频的原文件目录
     * @param countFilePath 含有词频计数的csv文件
     * @param wordIndex 词所在header的索引位置，从0起始
     * @param countIndex 计数字段所在header的索引位置，从0起始
     * @throws ServiceException 发生 IOException
     */
    public void countWord(String wordFilePath, String countFilePath, Integer wordIndex, Integer countIndex) throws ServiceException {
        Map<String, Integer> map = new HashMap<>(16);
        try (Reader in = new InputStreamReader(new FileInputStream(countFilePath), StandardCharsets.UTF_8)) {
            CSVParser csvParser = CSVFormat.DEFAULT.withHeader().withSkipHeaderRecord().parse(in);
            for (CSVRecord record : csvParser) {
                String word = record.get(wordIndex);
                int count = Integer.parseInt(record.get(countIndex));
                if (map.containsKey(word)){
                    map.put(word, map.get(word) + count);
                } else {
                    map.put(word, count);
                }
            }
        } catch (IOException e) {
            throw new ServiceException(e);
        }
        File wordDir = new File(wordFilePath);
        String[] files = wordDir.list((dir, name) -> StringUtils.endsWith(name, FileSuffixConstants.TXT));
        if (files == null || files.length == 0) {
            return;
        }
        for (String fileName : files) {
            String oriFilePath = wordFilePath + File.separator + fileName;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(oriFilePath),
                    StandardCharsets.UTF_8))) {
                File resultFileDir = new File(wordFilePath + File.separator + RESULT_FILE_NAME );
                resultFileDir.mkdir();
                String resultFile = wordFilePath + File.separator + RESULT_FILE_NAME + File.separator + fileName;
                Writer fileWriter = new OutputStreamWriter (new FileOutputStream (resultFile), StandardCharsets.UTF_8);
                try (BufferedWriter bufferWriter = new BufferedWriter(fileWriter)){
                    String line;
                    while ((line = reader.readLine()) != null) {
                        int count = 0;
                        if (map.containsKey(line)){
                            count = map.get(line);
                        }
                        bufferWriter.write(line + CommonConstants.COMMA + count + StringUtils.LF);
                    }
                }
            } catch (IOException e) {
                throw new ServiceException(e);
            }
        }
    }

    public void countWordByKey(String wordFilePath, String countFilePath, Integer keyIndex, Integer wordIndex, Integer countIndex) throws ServiceException {
        Map<String, Map<String, Integer>> wordMap = new HashMap<>(16);
        try (Reader in = new InputStreamReader(new FileInputStream(countFilePath), StandardCharsets.UTF_8)) {
            CSVParser csvParser = CSVFormat.DEFAULT.withHeader().withSkipHeaderRecord().parse(in);
            for (CSVRecord record : csvParser) {
                String key = record.get(keyIndex);
                String word = record.get(wordIndex);
                int count = Integer.parseInt(record.get(countIndex));
                if (wordMap.containsKey(word)){
                    Map<String, Integer> keyMap = wordMap.get(word);
                    keyMap.merge(key, count, (integer, integer2) -> integer + count);
                } else {
                    Map<String, Integer> keyMap = new HashMap<>();
                    keyMap.put(key, count);
                    wordMap.put(word, keyMap);
                }
            }
        } catch (IOException e) {
            throw new ServiceException(e);
        }
        File wordDir = new File(wordFilePath);
        String[] files = wordDir.list((dir, name) -> StringUtils.endsWith(name, FileSuffixConstants.TXT));
        if (files == null || files.length == 0) {
            return;
        }
        File resultFileDir = new File(wordFilePath + File.separator + RESULT_FILE_NAME );
        resultFileDir.mkdir();
        for (String fileName : files) {
            String oriFilePath = wordFilePath + File.separator + fileName;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(oriFilePath),
                    StandardCharsets.UTF_8))) {
                String resultFile = wordFilePath + File.separator + RESULT_FILE_NAME + File.separator + fileName;
                Writer fileWriter = new OutputStreamWriter (new FileOutputStream (resultFile), StandardCharsets.UTF_8);
                try (BufferedWriter bufferWriter = new BufferedWriter(fileWriter)){
                    String line;
                    Map<String, Integer> keyMap;
                    while ((line = reader.readLine()) != null) {
                        if (wordMap.containsKey(line)){
                            keyMap = wordMap.get(line);
                            for(Map.Entry<String, Integer> entry : keyMap.entrySet()){
                                bufferWriter.write(entry.getKey() + CommonConstants.COMMA + line + CommonConstants.COMMA + entry.getValue() + StringUtils.LF);
                            }
                        }
                    }
                }
            } catch (IOException e) {
                throw new ServiceException(e);
            }
        }
    }
}
