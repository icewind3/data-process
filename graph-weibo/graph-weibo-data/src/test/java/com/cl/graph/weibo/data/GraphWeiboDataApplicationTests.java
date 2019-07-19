package com.cl.graph.weibo.data;

import com.cl.graph.weibo.data.constant.SSDBConstants;
import org.apache.commons.lang3.time.DateUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(SpringRunner.class)
@SpringBootTest
public class GraphWeiboDataApplicationTests {

    @Resource(name ="userFriendsFollowersRedisTemplate")
    private StringRedisTemplate userFriendsFollowersRedisTemplate;

    @Resource(name = "userInfoRedisTemplate")
    private StringRedisTemplate userInfoRedisTemplate;

    @Test
    public void contextLoads() {
        String key = "show_v_friends_followers_lt_1w";
        ScanOptions scanOptions = ScanOptions.scanOptions().count(1000).match("*").build();
        AtomicInteger count = new AtomicInteger();
//        try (Cursor<Map.Entry<Object, Object>> cursor = userFriendsFollowersRedisTemplate.opsForHash().scan(key, scanOptions)){
//            cursor.forEachRemaining(entry -> {
//                Object key1 = entry.getKey();
//                System.out.println(entry);
//                count.getAndIncrement();
//            });
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        userFriendsFollowersRedisTemplate.execute((RedisConnection connection) -> {
            try (Cursor<byte[]> cursor = connection.scan(ScanOptions.scanOptions().count(Long.MAX_VALUE).match("*").build())) {
                cursor.forEachRemaining(item -> {
                    String s = new String(item, StandardCharsets.UTF_8);
                System.out.println(s);
                count.getAndIncrement();
            });
                return null;
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
        System.out.println(count.get());
    }

    @Test
    public void test() throws ParseException {
        String startDate = "20190501";
        String endDate = "20190503";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        Date date1 = simpleDateFormat.parse(startDate);
        Date date2 = simpleDateFormat.parse(endDate);
        while (!date1.after(date2)){
            System.out.println(simpleDateFormat.format(date1));
            date1 = DateUtils.addDays(date1, 1);
        }

    }


}
