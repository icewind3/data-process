package com.cl.data.process.util;

import com.cl.data.process.ao.DbContext;

/**
 * @author yejianyu
 * @date 2019/6/26
 */
public class DbContextHolder {

    private static final ThreadLocal<DbContext> DB_CONTEXT = new ThreadLocal<>();

    public static DbContext get(){
        return DB_CONTEXT.get();
    }

    public static void remove(){
        DB_CONTEXT.remove();
    }

    public static void set(DbContext value){
        DB_CONTEXT.set(value);
    }
}
