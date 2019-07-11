package com.cl.data.process.entity;

import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.io.Serializable;

/**
 * @author yejianyu
 * @date 2019/6/26
 */
@Entity
@Table(name = "mblog_from_uid")
public class MblogFromId implements Serializable {

    @Id
    private String mid;

    @Column(name = "uid")
    private Long uid;

    @Column(name = "text")
    private String text;

    public String getMid() {
        return mid;
    }

    public void setMid(String mid) {
        this.mid = mid;
    }

    public Long getUid() {
        return uid;
    }

    public void setUid(Long uid) {
        this.uid = uid;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("mid", mid)
                .append("uid", uid)
                .append("text", text)
                .toString();
    }
}
