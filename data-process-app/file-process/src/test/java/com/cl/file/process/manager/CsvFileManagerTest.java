package com.cl.file.process.manager;

import com.cl.data.process.core.constant.FileSuffixConstants;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/7/5
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class CsvFileManagerTest {

    @Autowired
    private CsvFileManager csvFileManager;

    @Test
    public void mergeFile() {
        String filePath = "C:/Users/cl32/Downloads";
        String[] header = new String[]{"id","gender","voteupCount","thankedCount"};
        File f = new File(filePath);
        String[] files = f.list((dir, name) -> StringUtils.startsWith(name, "user")
                && StringUtils.endsWith(name, FileSuffixConstants.CSV));
        String newFileName = "All_User";
        CsvFile csvFile = new CsvFile(filePath, newFileName, header);
        csvFileManager.mergeFile(csvFile, files);
    }

    @Test
    public void segmentFile() {
        String filePath = "C:/Users/cl32/Downloads";
        String[] header = new String[]{"id","gender","voteupCount","thankedCount"};
        File f = new File(filePath);
        String[] files = f.list((dir, name) -> StringUtils.startsWith(name, "user")
                && StringUtils.endsWith(name, FileSuffixConstants.CSV));
        String newFileName = "user";
        CsvFile csvFile = new CsvFile(filePath, newFileName, header);
        csvFileManager.segmentFile(csvFile, files, 6000000);
    }
}