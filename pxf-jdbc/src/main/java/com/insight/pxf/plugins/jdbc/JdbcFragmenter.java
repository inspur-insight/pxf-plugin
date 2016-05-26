package com.insight.pxf.plugins.jdbc;

import com.insight.pxf.cluster.PxfGroupListener;
import org.apache.hawq.pxf.api.Fragment;
import org.apache.hawq.pxf.api.Fragmenter;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.service.FragmenterFactory;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Random;

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
    //2个参数可以从pxf-site.xml中获取
    static int MAX_ROWS_PER_FRAG = 10;//每个分片的最大行数
    static int MAX_FRAGMENTS = 5;//最大分片数或当前"PXF实例数/2"

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


    /*为每个分片分配主机地址
      hosts中的主机是pxf进程所在的主机名，而不是目标数据库的主机名.
      pxf框架会调度任务到segment，由segment再连接pxf进程.
    */
    public static List<Fragment> assignHost(List<Fragment> fragments) throws Exception {
        String[] pxfmembers = PxfGroupListener.getPxfMembers();
        Random rand = new Random();
        for (Fragment fragment : fragments) {
            //主机名从PXF实例中随机选择
            String[] hosts = new String[]{pxfmembers[rand.nextInt(pxfmembers.length)]};
            fragment.setReplicas(hosts);
        }

        return fragments;
    }
}
