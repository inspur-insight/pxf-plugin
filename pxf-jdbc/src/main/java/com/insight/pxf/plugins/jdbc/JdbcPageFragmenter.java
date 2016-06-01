package com.insight.pxf.plugins.jdbc;

import com.insight.pxf.plugins.PageFragmentComputer;
import com.insight.pxf.plugins.jdbc.utils.DbProduct;
import com.insight.pxf.plugins.jdbc.utils.JdbcUtil;
import org.apache.hawq.pxf.api.Fragment;
import org.apache.hawq.pxf.api.UnsupportedTypeException;
import org.apache.hawq.pxf.api.utilities.InputData;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jiadx on 2016/5/13.
 * <p/>
 * 计算关系数据库表的分片-根据查询条件和参数（页数、行数），将结果分为若干页，每页是一个分片。
 *
 * 与HDFS、HBASE相比，关系数据库一般是一个实例，因此在分片时就有诸多限制：
 * 1.分片不能太多，因为每个分配相当于数据库服务的一个客户端，过多的客户端会造成数据库服务压力大。
 * 2.分片太少，则每个分片处理的数据太多，又会降低并发能力。
 * <p/>
 * 由此我们设计的分片方案如下：
 * 1.设置参数来限制每个分片的最大处理数据行数
 * 2.计算当前数据表的总行数，计算关系表分片数=总行数/每分片行数
 * <p/>
 * <p/>
 * 定义格式：
  LOCATION ('pxf://localhost:51200/demodb.myclass'
 '?PROFILE=JDBC'
 '&JDBC_DRIVER=com.mysql.jdbc.Driver'
 '&DB_URL=jdbc:mysql://192.168.200.6:3306/demodb&USER=root&PASS=root'
 '&FRAGMENTER=com.insight.pxf.plugins.jdbc.JdbcPageFragmenter'
 '&FRAGMENT_ROWS=10000'
 )
 其中FRAGMENTER、FRAGMENT_ROWS参数如果不定义则使用默认值。
 *
 * 其他分片方案：关系数据库自身也可能采用了分区、分表的技术，那么在进行Fragmenter切分时可以直接使用关系表的分片方案。
 * 本实现类是JDBC的默认分片算法。
 */
public class JdbcPageFragmenter extends JdbcFragmenter {
    PageFragmentComputer page_computer = null;

    public JdbcPageFragmenter(InputData inConf) {
        super(inConf);
        page_computer = new PageFragmentComputer(inConf);
    }

    /*数据库表的分片信息
      注：hosts中的主机是pxf进程所在的主机名，而不是目标数据库的主机名？还是目标数据库主机运行PXF？
      pxf框架会调度任务到segment，由segment再连接pxf进程.
    */
    @Override
    public List<Fragment> getFragments() throws Exception {
        //throw new UnsupportedOperationException("ANALYZE for JDBC plugin is not supported");
        Table table = new Table(inputData);
        try {
            table.open();

            int total_count = table.getCount();

            List<Fragment> pages = page_computer.computeFragments(total_count);
            fragments.addAll(pages);

        } finally {
            table.close();
        }

        return JdbcFragmenterFactory.assignHost(fragments);
    }

    public String buildFragmenterSql(String db_product, String origin_sql ){
        if(inputData.getFragmentMetadata() == null)
            return origin_sql;

        //分片查询信息：当前页起始行、每页行数
        int page_start = 0;
        int page_num = 0;

        //解析分片元数据
        byte[] meta = inputData.getFragmentMetadata();
        if(meta != null && meta.length == 8) {
            byte[][] newb = JdbcUtil.splitBytes(meta, 4);
            page_start = JdbcUtil.toInt(newb[0]);
            page_num = JdbcUtil.toInt(newb[1]);
        }

        DbProduct dbProduct = JdbcUtil.getDbProduct(db_product);
        return dbProduct.buildPageSql(origin_sql,page_start,page_num);
    }

}
