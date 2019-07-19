package com.cl.graph.weibo.data.util;

import com.cl.graph.weibo.data.entity.UserInfo;
import org.apache.commons.lang3.StringUtils;

/**
 * @author yejianyu
 * @date 2019/7/18
 */
public class UserInfoUtils {

    public static String getUserType(UserInfo userInfo) {
        if (userInfo == null){
            return StringUtils.EMPTY;
        }
        if (userInfo.isBlueV()) {
            return "blueV";
        } else {
            return "personal";
        }
    }

    public static String getPersonalUserType(UserInfo userInfo) {
        if (userInfo == null){
            return StringUtils.EMPTY;
        }
        if (userInfo.isPersonalUser()) {
            return "个人账户";
        }
        if (userInfo.isYellowV()) {
            return "黄v";
        }
        if (userInfo.isShowV()) {
            return "达人";
        }
        if (userInfo.isRedV()) {
            return "红v";
        }
        return StringUtils.EMPTY;
    }
}
