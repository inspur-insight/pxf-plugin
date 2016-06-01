package com.insight.pxf.plugins.jdbc.utils;

/**
 * Created by jiadx on 2016/6/1.
 */
public class OracleProduct implements DbProduct {

    /*构建分页查询语句
      参数：
        origin_sql ：原始sql
        page_start ：当前页起始行
        page_num   ：每页行数
     */
    public String buildPageSql(String origin_sql, int page_start, int page_num) {
        StringBuilder sb = new StringBuilder();
            /*oracle使用ROWNUM伪列方式,样例：
            SELECT * FROM (
                   SELECT A.*, ROWNUM RN
                   FROM (
                        SELECT * FROM TABLE_NAME
                        ) A
                   WHERE ROWNUM <= 40
            ) WHERE RN >= 21
            查询第21到40行
            */
        sb.append("SELECT * FROM ( ");
        sb.append("SELECT A.*,ROWNUM RN FROM ( ");
        sb.append(origin_sql);
        sb.append(" ) A");
        sb.append(" WHERE ROWNUM <= ").append(page_start + page_num);
        sb.append(" ) WHERE RN > ").append(page_start);
        return sb.toString();
    }

    //包装日期字符串为数据库日期类型
    public String wrapDate(Object date_val) {
        return "to_date('" + date_val + "','yyyy-mm-dd')";
    }
}
