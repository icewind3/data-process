package com.cl.data.process.interceptor;

import com.cl.data.process.ao.DbContext;
import com.cl.data.process.util.DbContextHolder;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.EmptyInterceptor;

/**
 * @author yejianyu
 * @date 2019/6/26
 */
public class JpaInterceptor extends EmptyInterceptor {

    @Override
    public String onPrepareStatement(String sql) {
        DbContext dbContext = DbContextHolder.get();
        if (dbContext != null && StringUtils.isNotBlank(dbContext.getSuffix())){
            return sql.replace(dbContext.getTableName(), dbContext.getTableName() + dbContext.getSuffix());
        }
        return super.onPrepareStatement(sql);
    }
}
