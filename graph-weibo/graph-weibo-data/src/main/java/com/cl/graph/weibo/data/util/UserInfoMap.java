package com.cl.graph.weibo.data.util;

import com.cl.graph.weibo.data.entity.UserInfo;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author yejianyu
 * @date 2019/7/18
 */
public class UserInfoMap {

    private static ConcurrentMap<String, UserInfo> userInfoMap = new ConcurrentHashMap<>();

    public static boolean contains(String uid){
        return userInfoMap.containsKey(uid);
    }

    public static UserInfo put(String uid, UserInfo userInfo){
        if (userInfo == null) {
            userInfo = new UserInfo();
        }
        return userInfoMap.put(uid, userInfo);
    }

    public static UserInfo get(String uid){
        return userInfoMap.get(uid);
    }

}
