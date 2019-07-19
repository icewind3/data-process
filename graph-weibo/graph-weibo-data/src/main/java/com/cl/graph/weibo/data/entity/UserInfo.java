package com.cl.graph.weibo.data.entity;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

/**
 * @author yejianyu
 * @date 2019/7/16
 */
@Data
public class UserInfo {

    private String uid;

    private String name;

    @JSONField(name = "followers_count")
    private Integer followersCount;

    @JSONField(name = "verified_type")
    private Integer verifiedType;

    @JSONField(name = "verified_type_ext")
    private Integer verifiedTypeExt;

    public boolean isBlueV(){
        return verifiedType != null && verifiedType >= 1 && verifiedType <= 7;
    }

    public boolean isPersonalBigV(){
        return verifiedType != null && verifiedType == 0;
    }

    public boolean isRedV(){
        return verifiedType != null && verifiedTypeExt != null && verifiedType == 0 && verifiedTypeExt == 1;
    }

    public boolean isYellowV(){
        return verifiedType != null && verifiedTypeExt != null && verifiedType == 0 && verifiedTypeExt == 0;
    }

    public boolean isShowV(){
        return verifiedType != null && (verifiedType == 200 || verifiedType == 220);
    }

    public boolean isPersonalUser(){
        return verifiedType != null && verifiedType == -1;
    }

    public boolean isCoreUser(){
        return isBlueV() || (followersCount != null && followersCount >= 10000);
    }

    public boolean isImportantUser(){
        return isShowV() || isPersonalBigV() || isCoreUser();
    }

}
