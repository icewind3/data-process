package com.cl.file.process;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;

import java.io.*;
import java.util.*;

/**
 * @author yejianyu
 * @date 2019/7/3
 */

public class WeiboDataProcessTest {

    private static final String ROOT_PATH = "C:/Users/cl32/Downloads/weibo/";

    private Logger logger = LoggerFactory.getLogger(WeiboDataProcessTest.class);

    @Test
    public void testRemoveDuplicates() throws IOException {
        for (int i = 14; i <= 25; i++) {
            doRemoveDuplicates(i);
        }

    }

    // 将Vertex_个人大v中的官方认证用户剔除
    private void doRemoveDuplicates(int index) throws IOException {

        Set<String> idSet = new HashSet<>();
        String[] header = new String[]{"ID"};
        String rootPath = ROOT_PATH + "temp/" + index;
        String readFilePath = rootPath + "/Vertex_官方认证用户.csv";
        try (Reader in = new FileReader(readFilePath)) {
            CSVParser csvParser = CSVFormat.DEFAULT.withHeader(header).withSkipHeaderRecord().parse(in);
            for (CSVRecord next : csvParser) {
                String id = next.get("ID");
                idSet.add(id);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        String readFilePath2 = rootPath + "/Vertex_个人大v.csv";
        try (Reader in = new FileReader(readFilePath2)) {
            CSVParser csvParser = CSVFormat.DEFAULT.withHeader(header).withSkipHeaderRecord().parse(in);
            String writeRootPath = ROOT_PATH + "new/" + index;
            File newDir = new File(writeRootPath);
            newDir.mkdir();
            File newFile = new File(writeRootPath + "/Vertex_个人大v.csv");
            try (FileWriter fileWriter = new FileWriter(newFile)) {
                CSVFormat csvFormat = CSVFormat.DEFAULT.withHeader(header);
                CSVPrinter csvFilePrinter = csvFormat.print(fileWriter);
                for (CSVRecord next : csvParser) {
                    String id = next.get("ID");
                    if (!idSet.contains(id)) {
                        List<String> row = new ArrayList<>();
                        for (String value : next) {
                            row.add("\"" + value + "\"");
                        }
                        csvFilePrinter.printRecord(row);
                    }
                }
            }
        }
    }
}
