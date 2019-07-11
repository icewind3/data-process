package com.cl.file.process.web.controller;

import com.cl.file.process.manager.CsvFile;
import com.cl.file.process.manager.CsvFileManager;
import com.cl.file.process.web.ResponseResult;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;

/**
 * @author yejianyu
 * @date 2019/7/9
 */
@RestController
@RequestMapping(value = "/csvFile")
public class CsvFileController {

    private final CsvFileManager csvFileManager;

    public CsvFileController(CsvFileManager csvFileManager) {
        this.csvFileManager = csvFileManager;
    }

    @RequestMapping(value = "/segmentFile")
    public ResponseResult filterVertexFile(@RequestParam String oriFilePath, @RequestParam Integer segmentSize) {
        File oriFile = new File(oriFilePath);
        if (oriFile.isDirectory()){
            String[] fileNames = oriFile.list();
            if (fileNames != null && fileNames.length != 0){
                for (String fileName : fileNames) {
                    CsvFile csvFile = new CsvFile(oriFile.getPath(), fileName);
                    csvFileManager.segmentFile(csvFile, segmentSize);
                }
            }
        } else {
            CsvFile csvFile = CsvFile.build(oriFile);
            csvFileManager.segmentFile(csvFile, segmentSize);
        }
        return ResponseResult.SUCCESS;
    }
}
