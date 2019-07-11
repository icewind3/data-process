package com.cl.data.process.web.controller;

import com.cl.data.process.ao.DbContext;
import com.cl.data.process.service.MbolgFileManager;
import com.cl.data.process.util.DbContextHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author yejianyu
 * @date 2019/6/26
 */
@RestController
@RequestMapping(value = "/mblog")
public class MblogController {

    private static final String MBLOG_FORM_UID_TABLE_NAME = "mblog_from_uid";
    private final MbolgFileManager mbolgFileManager;

    private final StringRedisTemplate redisTemplate;

    @Autowired
    public MblogController(MbolgFileManager mbolgFileManager, StringRedisTemplate redisTemplate) {
        this.mbolgFileManager = mbolgFileManager;
        this.redisTemplate = redisTemplate;
    }

    @PostMapping(value = "/genCsvFile")
    public Object genCsvFile(@RequestParam String path, @RequestParam String suffix){
        DbContext dbContext = new DbContext(MBLOG_FORM_UID_TABLE_NAME, suffix);
        DbContextHolder.set(dbContext);
        Long count;
        try {
            String fileName = MBLOG_FORM_UID_TABLE_NAME + suffix;
            count = mbolgFileManager.genCsvFile(path, fileName);
            mbolgFileManager.removeDuplicates(path, fileName);
        } catch (Exception e) {
            e.printStackTrace();
            return e.getMessage();
        } finally {
            DbContextHolder.remove();
        }
        return count;
    }

    @RequestMapping(value = "/removeDuplicates")
    public Object removeDuplicates(@RequestParam String path, @RequestParam String suffix){
        String fileName = MBLOG_FORM_UID_TABLE_NAME + "_" + suffix;
        mbolgFileManager.removeDuplicates(path, fileName);
        return "SUCCESS";
    }

    @RequestMapping(value = "/get")
    public Object get(String key){
        return redisTemplate.opsForSet().randomMember(key);
//        return redisTemplate.opsForValue().get(key);
    }

    @RequestMapping(value = "/set")
    public Object set(String key){
        return redisTemplate.opsForSet().add("a", key);
//        return redisTemplate.opsForValue().get(key);
    }

    @RequestMapping(value = "/delete")
    public Object delete(String key){
        return redisTemplate.opsForSet().remove("a", key);
//        return redisTemplate.delete(key);
    }
}
