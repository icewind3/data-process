package com.cl.graph.weibo.data.manager;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.cl.graph.weibo.data.constant.SSDBConstants;
import com.cl.graph.weibo.data.entity.UserInfo;
import com.cl.graph.weibo.data.util.UserInfoMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @author yejianyu
 * @date 2019/7/16
 */
@Slf4j
@Component
public class RedisDataManager {

    @Resource(name = "userFriendsFollowersRedisTemplate")
    private StringRedisTemplate userFriendsFollowersRedisTemplate;

    @Resource(name = "userInfoRedisTemplate")
    private StringRedisTemplate userInfoRedisTemplate;

    @Resource(name = "midUidRedisTemplate")
    private StringRedisTemplate midUidRedisTemplate;

    public String getUidByMid(String mid) {
        return (String) midUidRedisTemplate.opsForHash().get(SSDBConstants.SSDB_MID_UID_KEY, mid);
    }

    public UserInfo getUserInfo(String uid) {
        String value = (String) userInfoRedisTemplate.opsForHash().get(SSDBConstants.SSDB_USER_INFO_KEY, uid);
        try {
            return JSON.parseObject(value, UserInfo.class);
        } catch (JSONException e){
            log.error("json转UserInfo失败, json="+ value, e);
            e.printStackTrace();
            return null;
        }
    }

    public UserInfo getUserInfoViaCache(String uid){
        UserInfo userInfo;
        if (UserInfoMap.contains(uid)) {
            userInfo = UserInfoMap.get(uid);
        } else {
            userInfo = this.getUserInfo(String.valueOf(uid));
            UserInfoMap.put(uid, userInfo);
        }
        return userInfo;
    }

}
