package com.cl.graph.weibo.data.mapper.weibo;


import com.cl.graph.weibo.data.entity.FriendlyLinkTradeCompany;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**u
 * @author yejianyu
 * @date 2019/7/16
 */
@Mapper
public interface FriendlyLinkTradeCompanyMapper {

    FriendlyLinkTradeCompany findByUid(@Param("uid") String uid, @Param("suffix") String suffix);

}
