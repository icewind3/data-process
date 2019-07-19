package com.cl.graph.weibo.data.service;

import com.cl.graph.weibo.data.BaseSpringBootTest;
import org.apache.commons.lang3.time.DateUtils;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.text.ParseException;
import java.util.Date;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/7/17
 */
public class MblogFromUidServiceTest extends BaseSpringBootTest {

    @Autowired
    private MblogFromUidService mblogFromUidService;

    @Test
    public void genRetweetFile() {
        String resultFilePath = "C:/Users/cl32/Downloads/微博大图";
        String tableSuffix = "detail_2019031722";
        mblogFromUidService.genRetweetFile(resultFilePath, tableSuffix);
    }

    @Test
    public void test() throws ParseException {
        Date date = DateUtils.parseDate("20190603", "yyyyMMdd");
    }
}