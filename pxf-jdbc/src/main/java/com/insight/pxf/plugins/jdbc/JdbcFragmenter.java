package com.insight.pxf.plugins.jdbc;

import org.apache.hawq.pxf.api.Fragmenter;
import org.apache.hawq.pxf.api.utilities.InputData;

/**
 * Created by jiadx on 2016/5/13.
 * <p/>
 * 计算关系数据库表的分片。
 * 可支持多种数据库分片方案：
 * 1、数据库分页：根据查询条件和参数（页数、行数），将结果分为若干页，每页是一个分片。
 * 2.数据库分区：可以支持按照用户定义的分区列进行切分.
 * 3.数据库分表：暂不支持，可以在hawq定义多个表关联。
 */
public abstract class JdbcFragmenter extends Fragmenter {
    //参数可以从pxf-site.xml中获取
    static int FRAGMENT_ROWS = 100;//每个分片的最大行数

    public JdbcFragmenter(InputData inConf) {
        super(inConf);
    }

    public static String buildFragmenterSql(InputData inConf, String db_product, String origin_sql) throws Exception {
        JdbcFragmenter fragmenter = JdbcFragmenterFactory.createJdbcFragmenter(inConf);
        return fragmenter.buildFragmenterSql(db_product, origin_sql);
    }

    /* 根据分片信息构建sql
       参数：dbproduct - 数据库产品名称，来自jdbc connection元数据。
       sql - 原始sql
     */
    abstract String buildFragmenterSql(String db_product, String origin_sql);


}
