package com.cl.data.process.zhihu.web.controller;

import com.cl.data.process.zhihu.constant.TableNameConstants;
import com.cl.data.process.zhihu.manager.UserFileManager2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author yejianyu
 * @date 2019/7/2
 */
@RequestMapping(value = "/user")
@RestController
public class UserController {

    private static final String USER_TABLE_NAME = "user";

    @Autowired
    private UserFileManager2 userFileManager;

    @PostMapping(value = "/genCsvFile")
    public Object genCsvFile(@RequestParam String path, @RequestParam String tablePrefix){
        Long count;
        try {
            String fileName = TableNameConstants.USER + tablePrefix;
            count = userFileManager.genCsvFile(tablePrefix, path, fileName);
//            mbolgFileManager.removeDuplicates(path, fileName);
        } catch (Exception e) {
            e.printStackTrace();
            return e.getMessage();
        }
        return count;
    }

    @PostMapping(value = "/mergeCsvFile")
    public String mergeCsvFile(@RequestParam String path){
        try {
            userFileManager.mergeFile(path);
        } catch (Exception e) {
            e.printStackTrace();
            return e.getMessage();
        }
        return "success";
    }

    @PostMapping(value = "/segmentFile")
    public String segmentCsvFile(@RequestParam String path, @RequestParam Integer segmentSize){
        try {
            userFileManager.segmentFile(path, segmentSize);
        } catch (Exception e) {
            e.printStackTrace();
            return e.getMessage();
        }
        return "success";
    }
}
