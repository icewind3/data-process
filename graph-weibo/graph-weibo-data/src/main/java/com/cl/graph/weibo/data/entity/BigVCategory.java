package com.cl.graph.weibo.data.entity;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.math.BigInteger;
import java.sql.Date;

/**u
 * @author yejianyu
 * @date 2019/7/16
 */
@Data
public class BigVCategory {

    private String id;
    private BigInteger uid;
    private String navigationTwo;
    private String navigationThree;

    public String getMinNavigation(){
        if (StringUtils.isNotBlank(navigationThree)){
            return navigationThree;
        }
        return navigationTwo;
    }
}
