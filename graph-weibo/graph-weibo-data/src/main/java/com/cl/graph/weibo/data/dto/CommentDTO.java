package com.cl.graph.weibo.data.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.Value;

/**
 * @author yejianyu
 * @date 2019/7/17
 */
@ToString
@Getter
@Setter
public class CommentDTO {

    private String fromUid;
    private String toUid;
    private String mid;
    private String createTime;

    public CommentDTO(String fromUid, String toUid) {
        this.fromUid = fromUid;
        this.toUid = toUid;
    }
}
