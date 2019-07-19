package com.cl.graph.weibo.data.mapper.weibo;


import com.cl.graph.weibo.data.entity.BigVCategory;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**u
 * @author yejianyu
 * @date 2019/7/16
 */
@Mapper
public interface BigVCategoryMapper {

    List<BigVCategory> findCategoryByUid(@Param("uid") String uid);

    List<BigVCategory> findPeopleByUid(@Param("uid") String uid);
}
