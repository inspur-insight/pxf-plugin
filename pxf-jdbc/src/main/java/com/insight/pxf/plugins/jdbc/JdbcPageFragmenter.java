package com.insight.pxf.plugins.jdbc;

import com.insight.pxf.cluster.PxfGroupListener;
import com.insight.pxf.plugins.jdbc.Table;
import com.insight.pxf.plugins.jdbc.utils.JdbcUtil;
import org.apache.hawq.pxf.api.Fragment;
import org.apache.hawq.pxf.api.Fragmenter;
import org.apache.hawq.pxf.api.UnsupportedTypeException;
import org.apache.hawq.pxf.api.utilities.InputData;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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
 * 1.设置2个参数来限制：每个分片的最大处理数据行数、最大分片数
 * 2.计算当前数据表的总行数，计算关系表分片数=总行数/每分片行数
 * 3.如果当前分片数>最大分片数x2，则设置当前分片数=最大分片数x2，并重新计算每片的行数
 * <p/>
 * <p/>
 * 定义格式：
  LOCATION ('pxf://localhost:51200/demodb.myclass'
 '?PROFILE=JDBC'
 '&JDBC_DRIVER=com.mysql.jdbc.Driver'
 '&DB_URL=jdbc:mysql://192.168.200.6:3306/demodb&USER=root&PASS=root'
 '&FRAGMENTER=com.insight.pxf.plugins.jdbc.JdbcPageFragmenter'
 '&MAX_ROWS_PER_FRAG=10000&MAX_FRAGMENTS=10'
 )
 其中FRAGMENTER、MAX_ROWS_PER_FRAG、MAX_FRAGMENTS三个参数如果不定义则使用默认值。
 *
 * 其他分片方案：关系数据库自身也可能采用了分区、分表的技术，那么在进行Fragmenter切分时可以直接使用关系表的分片方案。
 * 本实现类是JDBC的默认分片算法。
 */
public class JdbcPageFragmenter extends JdbcFragmenter {
    int max_rows = MAX_ROWS_PER_FRAG;
    //最大分片数，如果计算的分片数大于此数值，则会调整每片的起止范围并重新计算。
    //实际最终的分片数会<此数值的2倍，如MAX=5，总记录数=9，每片记录数=1，则计算得到的分片数=9，则重新计算后取整的每片记录数还是1，最终的分片数仍然=9
    int max_frags = MAX_FRAGMENTS;

    public JdbcPageFragmenter(InputData inConf) {
        super(inConf);
        if(inConf.getUserProperty("MAX_ROWS_PER_FRAG") != null){
            max_rows = Integer.parseInt(inConf.getUserProperty("MAX_ROWS_PER_FRAG"));
        }
        if(inConf.getUserProperty("MAX_FRAGMENTS") != null){
            max_frags = Integer.parseInt(inConf.getUserProperty("MAX_FRAGMENTS") );
        }
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

            int count = table.getCount();

            List<int[]> pages = computeFragment(count);
            for (int[] pg : pages) {
                //分片元数据
                byte[] start = JdbcUtil.getBytes(pg[0]);
                byte[] nums = JdbcUtil.getBytes(pg[1]);
                byte[] fragmentMetadata = JdbcUtil.mergeBytes(start, nums);

                byte[] userData = new byte[0];
                Fragment fragment = new Fragment(inputData.getDataSource(), null, fragmentMetadata, userData);
                fragments.add(fragment);
            }

        } finally {
            table.close();
        }

        return assignHost(fragments);
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

        StringBuilder sb = new StringBuilder();
        if (db_product.toUpperCase().contains("MYSQL")) {
            //使用LIMIT分页查询--性能待调优
            sb.append(origin_sql).append(" ");
            sb.append("LIMIT ").append(page_start).append(",").append(page_num);
        } else if (db_product.toUpperCase().contains("ORACLE")) {
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
            sb.append(" WHERE ROWNUM <= ").append(page_start+page_num);
            sb.append(" ) WHERE RN > ").append(page_start);
        } else
            throw new UnsupportedTypeException("Unkwon Database Product: " + db_product );
        return sb.toString();
    }

    //计算分片数据，数组元素：[起始行,当前分片行数]
    public static List<int[]> computeFragment(int count) {
        int curr_rows = MAX_ROWS_PER_FRAG;
        int curr_pages = count / curr_rows +1;
        if (curr_pages > MAX_FRAGMENTS * 2) curr_pages = MAX_FRAGMENTS;
        curr_rows = count / curr_pages;

        List<int[]> pages = new ArrayList<>(curr_pages+1);
        for(int i =0;i<curr_pages;i++){
            int[] pg = new int[]{curr_rows*i,curr_rows};
            pages.add(pg);
        }
        if(curr_pages * curr_rows < count ){
            int[] pg = new int[]{curr_pages*curr_rows,count-(curr_pages * curr_rows)};
            pages.add(pg);
        }

        return pages;
    }

}
