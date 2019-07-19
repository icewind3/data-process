package com.cl.graph.weibo.data.web.controller;

import com.cl.graph.weibo.data.service.HotCommentService;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.concurrent.CountDownLatch;

/**
 * @author yejianyu
 * @date 2019/7/17
 */
@RequestMapping(value = "/comment")
@RestController
public class CommentController {

    @Resource(name = "commonThreadPoolTaskExecutor")
    private ThreadPoolTaskExecutor taskExecutor;

    private final HotCommentService hotCommentService;

    public CommentController(HotCommentService hotCommentService) {
        this.hotCommentService = hotCommentService;
    }

    @RequestMapping(value = "/init")
    public String initCommentEdge(@RequestParam(name = "resultPath") String resultPath){
        String[] dates = {"20190415","20190416","20190417","20190418"};
        CountDownLatch countDownLatch = new CountDownLatch(60);
        for (String date : dates){
            for (int i = 0; i < 15; i++) {
                String tableSuffix = date + "_" + i;
                taskExecutor.execute(() -> {
                    try {
                        hotCommentService.genCommentFile(resultPath, tableSuffix);
                    }finally {
                        countDownLatch.countDown();
                    }
                });
            }
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return "success";
    }
}
