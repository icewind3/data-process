package com.cl.data.process.util;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class CSVUtils {

    public static File createCSVFile(List<String> head, List<List<String>> dataList,
                                     String outPutPath, String filename) {

        File csvFile = null;
        BufferedWriter csvWriter = null;
        try {
            csvFile = new File(outPutPath + File.separator + filename + ".csv");
            File parent = csvFile.getParentFile();
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }
            csvFile.createNewFile();

            FileOutputStream fos = new FileOutputStream(csvFile);
            byte[] uft8bom={(byte)0xef,(byte)0xbb,(byte)0xbf};
            fos.write(uft8bom);

            csvWriter = new BufferedWriter(new OutputStreamWriter(fos, StandardCharsets.UTF_8), 1024);
            // 写入文件头部
            writeRow(head, csvWriter);

            // 写入文件内容
            for (List<String> row : dataList) {
                writeRow(row, csvWriter);
            }
            csvWriter.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                csvWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return csvFile;
    }

    private static void writeRow(List<String> row, BufferedWriter csvWriter) throws IOException {
        // 写入文件头部
        for (Object data : row) {
            String rowStr = "\"" + data + "\",";
            csvWriter.write(rowStr);
        }
        csvWriter.newLine();
    }


}
