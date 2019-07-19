package com.cl.graph.weibo.data.mapper.marketing;

import com.cl.graph.weibo.data.entity.MblogFromUid;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author yejianyu
 * @date 2019/7/16
 */
@Mapper
public interface MblogFromUidMapper {

    List<MblogFromUid> findAllRetweet(@Param("suffix") String tableSuffix, @Param("pageNum") int pageNum, @Param("pageSize") int pageSize);

    long countRetweet(@Param("suffix") String tableSuffix);
}
