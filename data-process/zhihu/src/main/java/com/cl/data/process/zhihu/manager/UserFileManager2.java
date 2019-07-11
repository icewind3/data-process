package com.cl.data.process.zhihu.manager;

import com.cl.data.process.zhihu.constant.TableNameConstants;
import com.cl.data.process.zhihu.csv.CsvFile;
import com.cl.data.process.zhihu.entity.User;
import com.cl.data.process.zhihu.exception.ServiceException;
import com.cl.data.process.zhihu.mapper.UserMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yejianyu
 * @date 2019/7/2
 */
@Service
public class UserFileManager2 extends AbstractFileManager {

    private static final String[] HEADER = new String[]{"id", "gender", "voteupCount", "thankedCount"};
    private static final String USER_KEY = "user_key";

    @Autowired
    private UserMapper userMapper;

    @Override
    protected long count(String tablePrefix) {
        return userMapper.count(tablePrefix);
    }

    public Long genCsvFile(String tablePrefix, String filePath, String fileName) throws Exception {
        CsvFile csvFile = new CsvFile(filePath, fileName, HEADER);
        return genCsvFile(tablePrefix, csvFile, pageRequest -> {
            List<User> userList = userMapper.findAll(tablePrefix, pageRequest.getPageNumber(), pageRequest.getPageSize());
            List<List<Object>> data = new ArrayList<>();
            for (User user : userList) {
                List<Object> row = new ArrayList<>();
                row.add(user.getId());
                row.add(user.getGender());
                row.add(user.getVoteupCount());
                row.add(user.getThankedCount());
                data.add(row);
            }
            return data;
        });
    }

    public void mergeFile(String filePath) {
        File f = new File(filePath);
        String[] files = f.list((dir, name) -> StringUtils.startsWith(name, TableNameConstants.USER)
                && StringUtils.endsWith(name, CsvFile.CSV_FILE_SUFFIX));
        String newFileName = "All_" + TableNameConstants.USER;
        CsvFile csvFile = new CsvFile(filePath, newFileName, HEADER);
        mergeFile(csvFile, files);
    }

    public void segmentFile(String filePath, int segmentSize) {
        File dirFile = new File(filePath);
        String[] files = dirFile.list((dir, name) -> StringUtils.startsWith(name, TableNameConstants.USER)
                   && StringUtils.endsWith(name, CsvFile.CSV_FILE_SUFFIX));
        CsvFile csvFile = new CsvFile(filePath, TableNameConstants.USER, HEADER);
        segmentFile(csvFile, files, segmentSize);
    }

}
