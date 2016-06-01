package com.insight.pxf.plugins.jdbc.utils;

/**
 * Created by jiadx on 2016/6/1.
 */
public class MysqlProduct implements DbProduct {

    /*构建分页查询语句
      参数：
        origin_sql ：原始sql
        page_start ：当前页起始行
        page_num   ：每页行数
     */
    public String buildPageSql(String origin_sql,int page_start,int page_num){
        StringBuilder sb = new StringBuilder();
        //使用LIMIT分页查询--性能待调优
        sb.append(origin_sql).append(" ");
        sb.append("LIMIT ").append(page_start).append(",").append(page_num);
        return sb.toString();
    }

    //包装日期字符串为数据库日期类型
    public String wrapDate(Object date_val){
        return "DATE('" + date_val + "')";
    }
}
