package com.cl.graph.weibo.data.web.controller;

import com.cl.graph.weibo.data.service.UserFileService;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author yejianyu
 * @date 2019/7/18
 */
@RequestMapping(value = "/user")
@RestController
public class UserController {

    private final UserFileService userFileService;

    public UserController(UserFileService userFileService) {
        this.userFileService = userFileService;
    }

    @RequestMapping(value = "/init")
    public String initUserVertex(@RequestParam(name = "key") String key,
                                    @RequestParam(name = "resultPath") String resultPath){
        userFileService.genUserFile(key, resultPath);
        return "success";
    }

    @RequestMapping(value = "/initByTraverse")
    public String initUserByTraverse(@RequestParam(name = "resultPath") String resultPath){
        userFileService.genUserFileByTraverse(resultPath);
        return "success";
    }
}
