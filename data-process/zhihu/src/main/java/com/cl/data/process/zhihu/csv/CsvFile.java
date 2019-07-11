package com.cl.data.process.zhihu.csv;

import java.util.List;

/**
 * @author yejianyu
 * @date 2019/7/2
 */
public class CsvFile {

    public static final String CSV_FILE_SUFFIX = ".csv";

    private String filePath;
    private String fileName;
    private String[] header;

    public CsvFile() {
    }

    public CsvFile(String filePath, String fileName) {
        this.filePath = filePath;
        this.fileName = fileName;
    }

    public CsvFile(String filePath, String fileName, String[] header) {
        this.filePath = filePath;
        this.fileName = fileName;
        this.header = header;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String[] getHeader() {
        return header;
    }

    public void setHeader(String[] header) {
        this.header = header;
    }
}
