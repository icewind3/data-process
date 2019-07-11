package com.cl.data.process.zhihu.mapper;

import com.cl.data.process.zhihu.entity.User;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author yejianyu
 * @date 2019/7/2
 */
@Mapper
public interface UserMapper {

    List<User> findAll(@Param("tablePrefix") String tablePrefix, @Param("pageNum") int pageNum, @Param("pageSize") int pageSize);

    long count(@Param("tablePrefix") String tablePrefix);
}
