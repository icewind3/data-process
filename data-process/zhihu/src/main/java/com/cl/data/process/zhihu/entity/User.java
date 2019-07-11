package com.cl.data.process.zhihu.entity;

import lombok.Data;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

/**
 * @author yejianyu
 * @date 2019/6/26
 */
@Data
public class User implements Serializable {

    private String id;

    private Integer gender;

    private String locations;

    private Integer voteupCount;
    private Integer thankedCount;
    private String school;
    private String major;



}
