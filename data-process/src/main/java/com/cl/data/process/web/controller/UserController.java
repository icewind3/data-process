package com.cl.data.process.web.controller;

import com.cl.data.process.manager.UserBlogFileFilterManager;
import com.cl.data.process.manager.UserFileManager;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author yejianyu
 * @date 2019/7/9
 */
@RestController
@RequestMapping(value = "/user")
public class UserController {

    private final UserFileManager userFileManager;
    private final UserBlogFileFilterManager userBlogFileFilterManager;

    public UserController(UserFileManager userFileManager, UserBlogFileFilterManager userBlogFileFilterManager) {
        this.userFileManager = userFileManager;
        this.userBlogFileFilterManager = userBlogFileFilterManager;
    }

    @RequestMapping(value = "/addLabel")
    public String addLabel(@RequestParam String filePath, @RequestParam String labelType){
        userFileManager.batchAddLabelToUser(filePath, labelType);
        return "success";
    }

    @RequestMapping(value = "/addMid")
    public String addLabel(@RequestParam String filePath){
        userFileManager.batchAddMidToUser(filePath);
        return "success";
    }

    @RequestMapping(value = "/filterBlog")
    public String filterBlog(@RequestParam String keyFilePath, @RequestParam String oriFilePath,
                             @RequestParam(defaultValue = "0") Integer filterIndex, @RequestParam String resultFilePath,
                             @RequestParam(defaultValue = "true") Boolean needInclude){
        userBlogFileFilterManager.filterFileByOneKey(keyFilePath, oriFilePath, filterIndex, resultFilePath, needInclude);
        return "success";
    }
}
