package com.insight.pxf.plugins.jdbc.utils;

import org.apache.hawq.pxf.api.UnsupportedTypeException;
import org.iq80.leveldb.DB;

/**
 * Created by jiadx on 2016/6/1.
 */
public interface DbProduct {

    /*构建分页查询语句
      参数：
        origin_sql ：原始sql
        page_start ：当前页起始行
        page_num   ：每页行数
     */
    String buildPageSql(String origin_sql,int page_start,int page_num);

    //包装日期字符串
    String wrapDate(Object date_val);
}
