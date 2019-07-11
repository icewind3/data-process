package com.cl.file.process.manager;

import com.cl.data.process.core.constant.FileSuffixConstants;
import com.cl.data.process.core.exception.ServiceException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * @author yejianyu
 * @date 2019/6/26
 */
@Service
public class CsvFileManager {

    private static final String SEGMENT_DIRECTORY = "segment";

    private Logger logger = LoggerFactory.getLogger(getClass());

    public void writeToCsvFile(List<List<Object>> content, CsvFile csvFile) throws ServiceException {
        String filePath = csvFile.getFilePath() + File.separator + csvFile.getFileName() + FileSuffixConstants.CSV;

        File file = new File(filePath);
        boolean needAppend = false;
        String[] headers = csvFile.getHeader();
        if (file.exists()) {
            try (Reader in = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)) {
                CSVParser csvParser = CSVFormat.DEFAULT.withHeader(headers).parse(in);
                Iterator<CSVRecord> iterator = csvParser.iterator();
                if (iterator.hasNext()) {
                    CSVRecord next = iterator.next();
                    for (int i = 0; i < headers.length; i++) {
                        if (!StringUtils.equals(next.get(i), headers[i])) {
                            throw new ServiceException("header不匹配，无法新增记录");
                        }
                    }
                }
            } catch (IOException e) {
                throw new ServiceException(e);
            }
            needAppend = true;
        }

        try (Writer fileWriter = new OutputStreamWriter(new FileOutputStream(file, needAppend), StandardCharsets.UTF_8)) {
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
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    public void mergeFile(CsvFile csvFile, String[] fileNames) throws ServiceException {
        if (fileNames == null || fileNames.length == 0) {
            return;
        }

        String filePath = csvFile.getFilePath();
        String mergeFileName = csvFile.getFileName();
        String[] header = csvFile.getHeader();
        File newFile = new File(filePath + File.separator + mergeFileName + FileSuffixConstants.CSV);

        try (Writer fileWriter = new OutputStreamWriter(new FileOutputStream(newFile), StandardCharsets.UTF_8)) {
            CSVPrinter csvPrinter = CSVFormat.DEFAULT.withHeader(header).print(fileWriter);
            for (String fileName : fileNames) {
                String readFilePath = filePath + File.separator + fileName;
                long startTime = System.currentTimeMillis();
                if (logger.isInfoEnabled()) {
                    logger.info("开始读取写入文件{}", readFilePath);
                }
                try (Reader in = new InputStreamReader(new FileInputStream(readFilePath), StandardCharsets.UTF_8)) {
                    CSVParser csvParser = CSVFormat.DEFAULT.withHeader(header).withSkipHeaderRecord().parse(in);
                    csvPrinter.printRecords(csvParser);
                }
                if (logger.isInfoEnabled()) {
                    logger.info("文件{}读取写入完成，共耗时{}ms", readFilePath, System.currentTimeMillis() - startTime);
                }
            }
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    public void segmentFile(CsvFile csvFile, String[] fileNames, int segmentSize) throws ServiceException {
        if (fileNames == null || fileNames.length == 0) {
            return;
        }
        String filePath = csvFile.getFilePath();
        String newFileName = csvFile.getFileName();
        String[] header = csvFile.getHeader();
        String segmentPath = filePath + File.separator + SEGMENT_DIRECTORY;
        File segmentFile = new File(segmentPath);
        segmentFile.mkdir();
        int index = 0;
        int count = 0;
        Writer fileWriter = null;
        long startTime = System.currentTimeMillis();
        try {
            CSVPrinter csvPrinter = null;
            File newFile = null;
            for (String fileName : fileNames) {
                try (Reader in = new InputStreamReader(new FileInputStream(filePath + File.separator + fileName),
                        StandardCharsets.UTF_8)) {
                    CSVParser csvParser = CSVFormat.DEFAULT.withHeader(header).withSkipHeaderRecord().parse(in);
                    for (CSVRecord record : csvParser) {
                        if (fileWriter == null || count >= segmentSize) {
                            if (fileWriter != null) {
                                if (logger.isInfoEnabled()) {
                                    logger.info("分割写入文件{}完成，耗时{}ms", newFile.getPath(), System.currentTimeMillis() - startTime);
                                }
                                fileWriter.close();
                            }
                            count = 0;
                            newFile = new File(segmentPath + File.separator + newFileName + index + FileSuffixConstants.CSV);
                            index++;
                            fileWriter = new OutputStreamWriter(new FileOutputStream(newFile), StandardCharsets.UTF_8);
                            csvPrinter = CSVFormat.DEFAULT.withHeader(header).print(fileWriter);
                            if (logger.isInfoEnabled()) {
                                startTime = System.currentTimeMillis();
                                logger.info("开始写入分割文件{}", newFile.getPath());
                            }
                        }
                        csvPrinter.printRecord(record);
                        count++;
                    }
                } catch (IOException e) {
                    throw new ServiceException(e);
                }
            }
        } finally {
            if (fileWriter != null) {
                try {
                    fileWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void segmentFile(CsvFile csvFile, int segmentSize) throws ServiceException {
        String filePath = csvFile.getFilePath();
        String fileName = csvFile.getFileName();
        String[] header = csvFile.getHeader();
        String segmentPath = filePath + File.separator + SEGMENT_DIRECTORY;
        File segmentFile = new File(segmentPath);
        segmentFile.mkdir();
        int index = 0;
        int count = 0;
        long startTime = System.currentTimeMillis();
        File oriFile = new File(filePath + File.separator + fileName);
        CSVFormat csvFormat;
        boolean hasHeader = header != null && header.length > 0;
        if (hasHeader){
            csvFormat = CSVFormat.DEFAULT.withHeader(header).withSkipHeaderRecord();
        } else {
            csvFormat = CSVFormat.DEFAULT;
        }
        CSVPrinter csvPrinter = null;
        try {
            File newFile = null;
                try (CSVParser csvParser = CSVParser.parse(oriFile, StandardCharsets.UTF_8, csvFormat)) {
                    for (CSVRecord record : csvParser) {
                        if (csvPrinter == null || count >= segmentSize) {
                            if (csvPrinter != null) {
                                if (logger.isInfoEnabled()) {
                                    logger.info("分割写入文件{}完成，耗时{}ms", newFile.getPath(), System.currentTimeMillis() - startTime);
                                }
                                csvPrinter.close();
                            }
                            count = 0;
                            newFile = new File(segmentPath + File.separator + fileName + index + FileSuffixConstants.CSV);
                            index++;
                            if (hasHeader){
                                csvPrinter = CSVFormat.DEFAULT.withHeader(header).print(newFile, StandardCharsets.UTF_8);
                            }else {
                                csvPrinter = CSVFormat.DEFAULT.print(newFile, StandardCharsets.UTF_8);
                            }
                            if (logger.isInfoEnabled()) {
                                startTime = System.currentTimeMillis();
                                logger.info("开始写入分割文件{}", newFile.getPath());
                            }
                        }
                        csvPrinter.printRecord(record);
                        count++;
                    }
                } catch (IOException e) {
                    throw new ServiceException(e);
                }
        } finally {
            if (csvPrinter != null) {
                try {
                    csvPrinter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
