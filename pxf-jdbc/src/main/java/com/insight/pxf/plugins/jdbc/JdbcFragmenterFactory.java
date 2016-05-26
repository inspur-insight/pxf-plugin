package com.insight.pxf.plugins.jdbc;

import com.insight.pxf.cluster.PxfGroupListener;
import org.apache.hawq.pxf.api.Fragment;
import org.apache.hawq.pxf.api.Fragmenter;
import org.apache.hawq.pxf.api.FragmentsStats;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.service.FragmenterFactory;

import java.util.List;
import java.util.Random;

/**
 * Created by jiadx on 2016/5/13.
 * <p/>
 * 计算关系数据库表的分片的工厂类。可根据参数自动选择适合的JdbcFragmenter。
 *
 * 可支持多种数据库分片方案：
 * 1、数据库分页：根据查询条件和参数（页数、行数），将结果分为若干页，每页是一个分片。
 * 2.数据库分区：可以支持按照用户定义的分区列进行切分.
 * 3.数据库分表：暂不支持，可以在hawq定义多个表关联。
 *
 */
public class JdbcFragmenterFactory extends Fragmenter {
    JdbcFragmenter fragmenter = null;

    public JdbcFragmenterFactory(InputData inConf) {
        super(inConf);
        fragmenter = createJdbcFragmenter(inConf);
    }

    @Override
    public List<Fragment> getFragments() throws Exception {
        return fragmenter.getFragments();
    }
    @Override
    public FragmentsStats getFragmentsStats() throws Exception {
        return fragmenter.getFragmentsStats();
    }
    public static JdbcFragmenter createJdbcFragmenter(InputData inputData){
        if(inputData.getUserProperty("PARTITION_BY") != null )
            return new JdbcPartitionFragmenter(inputData);
        else
            return new JdbcPageFragmenter(inputData);
    }
}
