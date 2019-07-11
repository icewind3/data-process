package com.cl.data.process.ao;

/**
 * @author yejianyu
 * @date 2019/6/26
 */
public class DbContext {

    private String tableName;
    private String suffix;

    public DbContext(){
    }

    public DbContext(String tableName, String suffix) {
        this.tableName = tableName;
        this.suffix = suffix;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSuffix() {
        return suffix;
    }

    public void setSuffix(String suffix) {
        this.suffix = suffix;
    }
}
