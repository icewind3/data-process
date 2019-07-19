package com.cl.graph.weibo.data.service;

import com.alibaba.fastjson.JSON;
import com.cl.graph.weibo.data.BaseSpringBootTest;
import com.cl.graph.weibo.data.dto.FollowingRelationshipDTO;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author yejianyu
 * @date 2019/7/18
 */
public class FollowingRelationshipServiceTest extends BaseSpringBootTest {

    @Autowired
    private FollowingRelationshipService followingRelationshipService;

    @Test
    public void genFollowingRelationshipFile() {
        String file = "C:/Users/cl32/Downloads/微博大图/attention/20181229";
        String resultFile = "C:/Users/cl32/Downloads/微博大图";
       followingRelationshipService.genFollowingRelationshipFile(file, resultFile);

    }
}