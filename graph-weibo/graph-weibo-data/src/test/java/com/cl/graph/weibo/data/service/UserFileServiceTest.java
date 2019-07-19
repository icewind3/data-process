package com.cl.graph.weibo.data.service;

import com.alibaba.fastjson.JSON;
import com.cl.graph.weibo.data.entity.UserInfo;
import com.cl.graph.weibo.data.util.CsvFileHelper;
import org.apache.commons.csv.CSVPrinter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nutz.ioc.IocException;
import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/7/16
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class UserFileServiceTest {

    @Autowired
    private UserFileService userFileService;

    @Test
    public void genUserFile() {
        String resultFilePath = "C:/Users/cl32/Downloads/微博大图";

//                show_v_friends_followers_lt_1w
        String[] keys = new String[]{"show_v_friends_followers_gte_1w_lt_5w", "show_v_friends_followers_lt_1w"};
        for (String key : keys) {
            long count = userFileService.genUserFile(key, resultFilePath);
            System.out.println(key + ", count=" + count);
        }

    }

    @Test
    public void test() {

        try ( SSDB ssdb = SSDBs.pool("192.168.2.16", 8889, 1000 * 100, null);
            CSVPrinter csvPrinter = CsvFileHelper.writer("aa")){
            String start = "";
            String end = "";
            final long[] max = {0};
            long index = 0;
            int stepSize = 1000;
            while (true) {
                if (max[0] != 0){
                    start = String.valueOf(max[0]);
                }
                Response response = ssdb.hscan("userinfo", start, end, stepSize);
                Map<String, String> map = response.mapString();
                map.forEach((k, v) -> {
                    UserInfo userInfo = JSON.parseObject(v, UserInfo.class);
                    max[0] = Long.parseLong(userInfo.getUid());
                    try {
                        csvPrinter.printRecord(userInfo.getUid(), userInfo.getName());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    System.out.println(userInfo);
                });
                System.out.println(++index);
                if (map.size() < stepSize || index > 410000){
                    break;
                }
                map.clear();
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }

}