package com.cl.data.process.zhihu.mapper;

import com.cl.data.process.zhihu.entity.User;
import com.cl.data.process.zhihu.entity.UserEducations;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author yejianyu
 * @date 2019/7/2
 */
@Mapper
public interface UserEducationsMapper {

    UserEducations getByUserId(@Param("uid") int uid);


}
