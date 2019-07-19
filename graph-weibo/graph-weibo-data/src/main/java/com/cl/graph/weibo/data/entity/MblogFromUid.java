package com.cl.graph.weibo.data.entity;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

/**
 * @author yejianyu
 * @date 2019/7/17
 */
@Data
public class MblogFromUid {

    private String mid;
    private String uid;
    private String pid;
    private String retweetedMid;
    private String createTime;
}
