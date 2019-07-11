package com.cl.data.process;

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
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author yejianyu
 * @date 2019/7/3
 */

public class WeiboDataProcessTest {

    private static final String ROOT_PATH = "C:/Users/cl32/Downloads/weibo2/";
    private static final String EDGE_PATH = "C:/Users/cl32/Downloads/weibo/edge/";

    private Logger logger = LoggerFactory.getLogger(WeiboDataProcessTest.class);

    @Test
    public void testWordProcess() {
        Map<String, Integer> map = new HashMap<>();
        String[] header = new String[]{"FROM","TO","count","INNER_EDGE_ID"};
        String rootPath = "C:/Users/cl32/Documents/分群结果_product/";
//        String typePath = "allbrids/直接粉丝-产品二分图";
        String typePath = "Leo/Leo视频触达人群的-产品二分图";
//        String typePath = "organics/Organic KOL-品牌二分图";
        String dirPath = rootPath + typePath + File.separator +  "product";
        String readFilePath = dirPath + File.separator + "Edge_用户-博文标签-interest.csv";
        try (Reader in = new FileReader(readFilePath)) {
            CSVParser csvParser = CSVFormat.DEFAULT.withHeader(header).withSkipHeaderRecord().parse(in);
            for (CSVRecord record : csvParser) {
                String label = record.get("TO");
                int count = Integer.parseInt(record.get("count"));
               if (map.containsKey(label)){
                   map.put(label, map.get(label) + count);
               } else {
                   map.put(label, count);
               }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        File productDir = new File(dirPath);
        String[] files = productDir.list((dir, name) -> StringUtils.endsWith(name, ".txt"));
        for (String fileName : files) {
            String filePath = dirPath + File.separator + fileName;
            try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                File resultFileDir = new File(dirPath + File.separator + "result" );
                resultFileDir.mkdir();
                String resultFile = dirPath + File.separator + "result" + File.separator + fileName;
                FileWriter fileWriter = new FileWriter(resultFile);
                try (BufferedWriter bufferWriter = new BufferedWriter(fileWriter)){
                    String line;
                    while ((line = reader.readLine()) != null) {
                        int count = 0;
                        if (map.containsKey(line)){
                            count = map.get(line);
                        }
                        bufferWriter.write(line + "," + count + "\n");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void doFilter() throws IOException {

        Set<String> idSet = new HashSet<>();
        String[] header = new String[]{"uid","label","count"};
        String rootPath = "C:/Users/cl32/Documents/uid-product/part";
        File readFile = new File(rootPath + "/weibo_followers__10_w.csv");

        try (CSVParser csvParser = CSVParser.parse(readFile, StandardCharsets.UTF_8,
                CSVFormat.DEFAULT.withHeader(header).withSkipHeaderRecord())) {
            String writeRootPath = rootPath + "/new";
            File newDir = new File(writeRootPath);
            newDir.mkdir();
            File newFile = new File(writeRootPath + "/weibo_followers__10_w.csv");
            try (CSVPrinter csvFilePrinter = CSVFormat.DEFAULT.withHeader(header).print(newFile, StandardCharsets.UTF_8)) {
                for (CSVRecord record : csvParser) {
                    String id = record.get("uid");
                    if (id.startsWith("1") || id.startsWith("2")){
                        csvFilePrinter.printRecord(record);
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

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
        String rootPath = ROOT_PATH + "vertex/" + index;
//        String rootPath = ROOT_PATH + "vertex";
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
//            String writeRootPath = ROOT_PATH + "new";
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

    // 将边文件中 已是官方认证用户的 个人大v 剔除
    @Test
    public void doRemoveDuplicatesEdge() throws IOException {

        Set<String> idSet = new HashSet<>();
        String[] header1 = new String[]{"ID"};
        String rootPath = "C:/Users/cl32/Downloads/weibo2";
        File readFilePath = new File(rootPath + "/newVertex/Vertex_个人大v.csv");
        try (CSVParser csvParser = CSVParser.parse(readFilePath, StandardCharsets.UTF_8,
                CSVFormat.DEFAULT.withHeader(header1).withSkipHeaderRecord())) {
            for (CSVRecord next : csvParser) {
                String id = next.get("ID");
                idSet.add(id);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        int count1 = 0;
        int count2 = 0;
        String filterFileName = "person_offper_reposts.csv";
        File readFilePath2 = new File(rootPath + "/edge/" + filterFileName);
        try (CSVParser csvParser = CSVParser.parse(readFilePath2, StandardCharsets.UTF_8,
                CSVFormat.DEFAULT)) {
            String writeRootPath = rootPath + "/edge/new";
            File newDir = new File(writeRootPath);
            newDir.mkdir();
            File newFile = new File(writeRootPath + File.separator + filterFileName);
            try (CSVPrinter csvFilePrinter = CSVFormat.DEFAULT.print(newFile,
                    StandardCharsets.UTF_8)) {
                Iterator<CSVRecord> iterator = csvParser.iterator();
                if (iterator.hasNext()) {
                    CSVRecord header = iterator.next();
                    csvFilePrinter.printRecord(header);
                }
                while (iterator.hasNext()) {
                    CSVRecord record = iterator.next();
                    count1++;
                    if (idSet.contains(record.get(0))){
                        count2++;
                        System.out.println(record.get(0));
                        csvFilePrinter.printRecord(record);
                    }
                }
            }
            System.out.println(count1 + "=" + count2);
        }
    }

    @Test
    public void weiboFileProcess() throws IOException {
        doWeiboFileProcess(0);
    }

    private void doWeiboFileProcess(int index) throws IOException {
        String uidFilePath = ROOT_PATH + "图三uid/" + index + ".txt";

        Set<String> idSet = new HashSet<>();
        FileSystemResource resource = new FileSystemResource(uidFilePath);
        BufferedReader br = new BufferedReader(new FileReader(resource.getFile()));
        String str;
        int count = 0;
        while ((str = br.readLine()) != null) {
            if (idSet.contains(str)) {
                System.out.println(str);
                count++;
                continue;
            }
            idSet.add(str);
        }
        String newPath = ROOT_PATH + index;
        File newDir = new File(newPath);
        if (newDir.mkdir()) {
            genVertexFile(idSet, newPath, "Vertex_个人大v.csv");
            genVertexFile(idSet, newPath, "Vertex_个人认证用户.csv");
            genVertexFile(idSet, newPath, "Vertex_官方认证用户.csv");
            genEdgeFile(idSet, newPath);
        }
        System.out.println(count);
    }

    private void genVertexFile(Set<String> idSet, String path, String fileName) {
        long startTime = System.currentTimeMillis();
        String[] header = new String[]{"ID"};
        String readFilePath = ROOT_PATH + "vertex/" + fileName;
        try (Reader in = new FileReader(readFilePath)) {
            CSVParser csvParser = CSVFormat.DEFAULT.withHeader(header).withSkipHeaderRecord().parse(in);
            Iterator<CSVRecord> readIterator = csvParser.iterator();

            File newFile = new File(path + File.separator + fileName);
            try (FileWriter fileWriter = new FileWriter(newFile)) {
                CSVFormat csvFormat = CSVFormat.DEFAULT.withHeader(header);
                CSVPrinter csvFilePrinter = csvFormat.print(fileWriter);
                while (readIterator.hasNext()) {
                    CSVRecord next = readIterator.next();
                    String id = next.get("ID");
                    if (idSet.contains(id)) {
                        List<String> row = new ArrayList<>();
                        for (String value : next) {
                            row.add(value);
                        }
                        csvFilePrinter.printRecord(row);
                    }
                }
            }
            logger.info("生成点文件{}完成, 共耗时{}ms", newFile.getPath(), System.currentTimeMillis() - startTime);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void genEdgeFile(Set<String> idSet, String path) {

        File edgeDir = new File(EDGE_PATH);
        String[] files = edgeDir.list((dir, name) -> StringUtils.endsWith(name, ".csv"));
        if (files == null) {
            return;
        }
        for (String file : files) {
            long startTime = System.currentTimeMillis();
            String readFilePath = EDGE_PATH + file;
            try (Reader in = new FileReader(readFilePath)) {
                CSVParser csvParser = CSVFormat.DEFAULT.parse(in);
                Iterator<CSVRecord> readIterator = csvParser.iterator();
                CSVRecord headerRecord = readIterator.next();
                int headSize = headerRecord.size();
                String[] header = new String[headSize];
                for (int i = 0; i < headSize; i++) {
                    header[i] = headerRecord.get(i);
                }
                File newFile = new File(path + File.separator + file);
                try (FileWriter fileWriter = new FileWriter(newFile)) {
                    CSVFormat csvFormat = CSVFormat.DEFAULT.withHeader(header);
                    CSVPrinter csvFilePrinter = csvFormat.print(fileWriter);
                    while (readIterator.hasNext()) {
                        CSVRecord next = readIterator.next();
                        String fromId = next.get(0);
                        String toId = next.get(1);
                        if (idSet.contains(fromId) && idSet.contains(toId)) {
                            List<String> row = new ArrayList<>();
                            for (String value : next) {
                                row.add(value);
                            }
                            csvFilePrinter.printRecord(row);
                        }
                    }
                }
                logger.info("生成边文件{}完成, 共耗时{}ms", newFile.getPath(), System.currentTimeMillis() - startTime);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
