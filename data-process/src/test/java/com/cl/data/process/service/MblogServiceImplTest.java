package com.cl.data.process.service;

import com.cl.data.process.entity.MblogFromId;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.*;
import java.util.*;

/**
 * @author yejianyu
 * @date 2019/6/26
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class MblogServiceImplTest {

    @Autowired
    private MblogServiceImpl mblogService;

    @Test
    public void testRead() throws IOException {
        String filePath = "C:/Users/cl32/Downloads/mblog.csv";
        String[] head = new String[]{"mid", "uid", "text"};
        File file = new File(filePath);
        if (file.exists()){
            List<MblogFromId> all = mblogService.findAll();
            Reader in = new FileReader(file);
            CSVParser csvParser = CSVFormat.DEFAULT.withHeader(head).withSkipHeaderRecord().parse(in);
            List<CSVRecord> records = csvParser.getRecords();
            int line = 0;
            for (CSVRecord record : records) {
                String mid = all.get(line).getMid();
                if (!StringUtils.equals(record.get("mid"),mid)){
                    System.out.println(String.format("记录不相同，line=%d, mid=%s", line ,mid));
                }
                line++;
            }
            System.out.println(line);
        }
    }

    @Test
    public void test() throws IOException {
        int size = 10000;
        long maxCount = mblogService.count();
        long startTime = System.currentTimeMillis();
        int page = 0;
        int count = 0;
        while (count < maxCount) {
            writeToCSVFile(page, size);
            count += size;
            page++;
        }
        System.out.println("共写入" + page + "页, 共耗时" + (System.currentTimeMillis() - startTime)+ "ms");
    }

    private void writeToCSVFile(int page, int size) throws IOException {
        long startTime = System.currentTimeMillis();
        List<MblogFromId> mblogList = mblogService.findAll(page, size);

        String[] head = new String[]{"mid", "uid", "text"};

        String filePath = "C:/Users/cl32/Downloads/mblog.csv";
        File file = new File(filePath);
        boolean needAppend = false;
        if (file.exists()){
            Reader in = new FileReader(file);
            CSVParser csvParser = CSVFormat.DEFAULT.withHeader(head).parse(in);
            Iterator<CSVRecord> iterator = csvParser.iterator();
            if (iterator.hasNext()){
                CSVRecord next = iterator.next();
                for (int i = 0; i < head.length; i++) {
                    if (!StringUtils.equals(next.get(i), head[i])){
                        throw new RuntimeException("header不匹配， 无法新增记录");
                    }
                }
            }
            needAppend = true;
        }

        try (FileWriter fileWriter = new FileWriter(file, needAppend)){
            CSVFormat csvFormat;
            if (needAppend) {
                csvFormat = CSVFormat.DEFAULT;
            } else {
                csvFormat = CSVFormat.DEFAULT.withHeader(head);
            }
            CSVPrinter csvFilePrinter = csvFormat.print(fileWriter);
            for (MblogFromId mblog : mblogList) {
                List<Object> row = new ArrayList<>();
                row.add(mblog.getMid());
                row.add(mblog.getUid());
                row.add(mblog.getText());
                csvFilePrinter.printRecord(row);
            }
        }
        System.out.println("写入page="+ page +", 耗时" + (System.currentTimeMillis() - startTime)+ "ms");
    }
}