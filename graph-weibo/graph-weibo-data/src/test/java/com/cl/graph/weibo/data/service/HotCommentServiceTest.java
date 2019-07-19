package com.cl.graph.weibo.data.service;

import com.cl.graph.weibo.data.BaseSpringBootTest;
import com.cl.graph.weibo.data.mapper.marketing.HotCommentMapper;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;

/**
 * @author yejianyu
 * @date 2019/7/17
 */
public class HotCommentServiceTest extends BaseSpringBootTest {

    @Autowired
    private HotCommentService hotCommentService;

    @Resource
    private HotCommentMapper hotCommentMapper;


    @Test
    public void genCommentFile() {
        String resultFilePath = "C:/Users/cl32/Downloads/微博大图";
        String tableSuffix = "20190415_0";
        hotCommentService.genCommentFile(resultFilePath, tableSuffix);
    }
}