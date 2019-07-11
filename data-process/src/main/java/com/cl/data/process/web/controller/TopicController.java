package com.cl.data.process.web.controller;

import com.cl.data.process.ao.DbContext;
import com.cl.data.process.service.TopicService;
import com.cl.data.process.util.DbContextHolder;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author yejianyu
 * @date 2019/7/8
 */
@RestController
@RequestMapping(value = "/topic")
public class TopicController {

    private static final String TOPIC_TABLE_NAME = "topic";
    private final TopicService topicService;

    public TopicController(TopicService topicService) {
        this.topicService = topicService;
    }

    @RequestMapping(value = "/classify")
    public String classifyTopic(@RequestParam String path, @RequestParam(required = false) String suffix){
        DbContext dbContext = new DbContext(TOPIC_TABLE_NAME, suffix);
        DbContextHolder.set(dbContext);
        try {
            String fileName = StringUtils.isBlank(suffix) ? TOPIC_TABLE_NAME : TOPIC_TABLE_NAME + suffix;
            topicService.classifyTopicToFile(path, fileName);
        } catch (Exception e) {
            e.printStackTrace();
            return e.getMessage();
        } finally {
            DbContextHolder.remove();
        }
        return "success";
    }
}
