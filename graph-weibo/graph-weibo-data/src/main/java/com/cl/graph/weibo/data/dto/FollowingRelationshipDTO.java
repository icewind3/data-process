package com.cl.graph.weibo.data.dto;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author yejianyu
 * @date 2019/7/18
 */
@ToString
@Getter
@Setter
public class FollowingRelationshipDTO {

    @JSONField(name = "master")
    private Long to;

    @JSONField(name = "slave")
    private Long from;
}
