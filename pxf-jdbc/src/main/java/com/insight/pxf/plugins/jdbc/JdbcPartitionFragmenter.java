package com.insight.pxf.plugins.jdbc;

import com.insight.pxf.plugins.jdbc.Table;
import com.insight.pxf.plugins.jdbc.utils.JdbcUtil;
import org.apache.hawq.pxf.api.Fragment;
import org.apache.hawq.pxf.api.Fragmenter;
import org.apache.hawq.pxf.api.UnsupportedTypeException;
import org.apache.hawq.pxf.api.utilities.InputData;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by jiadx on 2016/5/13.
 * <p/>
 * 基于数据库分区key来计算关系数据库表的分片。
 * 分区key支持日期型、整型、枚举型。
 * 其中：
 * 1.日期型：用户需要设置总的起止时间、每个分片的间隔时间。
 * 2.整型：需要设置起止数值，每个分片的间隔量。
 * 3.枚举型：直接定义一个字符串的集合，其中的每个字符串就是每个分片的关键字。
 *
 * 定义格式-日期型：
 LOCATION ('pxf://localhost:51200/demodb.myclass'
 '?PROFILE=JDBC'
 '&JDBC_DRIVER=com.mysql.jdbc.Driver'
 '&DB_URL=jdbc:mysql://192.168.200.6:3306/demodb&USER=root&PASS=root'
 '&FRAGMENTER=com.insight.pxf.plugins.jdbc.JdbcPartitionFragmenter'
 '&PARTITION_BY=createdate:date&RANGE=2008-01-01:2010-01-01&INTERVAL=1:day'
 )
 PARTITION_BY参数：为关系数据库的列名和类型（可以不在hawq中定义），冒号分隔，类型：date、int、enum
 RANGE参数：设置起始日期，格式yyyy-MM-dd，冒号分隔。其中RANGE中的范围是左封闭，即：>=start AND < end
 INTERVAL参数：分片间隔，可支持：day、month、year，前面可加整数值，冒号分隔

 整型定义：
 PARTITION_BY=year:int&RANGE=2008:2010&INTERVAL=1

 枚举型：
 PARTITION_BY=level:enum&RANGE=excellent:good:general:bad

 系统也可自动检查数据库表列类型，并自动判断应用格式？
 *
 */
public class JdbcPartitionFragmenter extends JdbcFragmenter {
    String[] partition_by = null;
    String[] range = null;
    String[] interval = null;
    PartitionType partitionType = null;
    String partitionColumn = null;
    IntervalType intervalType = null;
    int intervalVal = 0;

    enum PartitionType {
        DATE,
        INT,
        ENUM;

        public static PartitionType getType(String str) {
            return valueOf(str.toUpperCase());
        }
    }

    enum IntervalType {
        DAY,
        MONTH,
        YEAR;

        public static IntervalType type(String str) {
            return valueOf(str.toUpperCase());
        }
    }

    //以毫秒为单位记录的单位间隔，用于日期分区类型时估计分片数量
    static Map<IntervalType, Long> intervals = new HashMap<IntervalType, Long>();

    static {
        intervals.put(IntervalType.DAY, (long) 24 * 60 * 60 * 1000);
        intervals.put(IntervalType.MONTH, (long) 30 * 24 * 60 * 60 * 1000);//按30天计算
        intervals.put(IntervalType.YEAR, (long) 365 * 30 * 24 * 60 * 60 * 1000);//按365天计算
    }


    public JdbcPartitionFragmenter(InputData inConf) {
        super(inConf);
        partition_by = inConf.getUserProperty("PARTITION_BY").split(":");
        partitionColumn = partition_by[0];
        partitionType = PartitionType.getType(partition_by[1]);

        range = inConf.getUserProperty("range").split(":");

        if (inConf.getUserProperty("interval") != null) {
            interval = inConf.getUserProperty("interval").split(":");
            intervalVal = Integer.parseInt(interval[0]);
            if (interval.length > 1)
                intervalType = IntervalType.type(interval[1]);
        }
        /*TODO 参数值校验：
            1.PARTITION_BY、RANGE参数是必须的
            2.日期型、整型必须设置 INTERVAL 参数
            3.RANGE参数值必须以冒号分隔，需要符合类型要求的格式-日期、整数，
            4.
         */
    }

    @Override
    public List<Fragment> getFragments() throws Exception {
        switch (partitionType) {
            case DATE: {
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                Date t_start = df.parse(range[0]);
                Date t_end = df.parse(range[1]);
                int curr_interval = intervalVal;
                long curr_frags = (t_end.getTime() - t_start.getTime()) / (curr_interval * intervals.get(intervalType));
                if (curr_frags > MAX_FRAGMENTS) {
                    //调整间隔时间 = 总时间范围/最大分片数 (毫秒单位)
                    //间隔单位=间隔毫秒时间/单位时间
                    curr_interval = (int) (((t_end.getTime() - t_start.getTime()) / MAX_FRAGMENTS) / intervals.get(intervalType));
                    //校验:curr_interval > 0，主要是在long转int时会出现负值
                }
                Calendar frag_start = Calendar.getInstance();
                Calendar c_end = Calendar.getInstance();
                frag_start.setTime(t_start);
                c_end.setTime(t_end);
                while (frag_start.before(c_end) ){//|| frag_start.compareTo(c_end) == 0) {
                    Calendar frag_end = (Calendar) frag_start.clone();
                    switch (intervalType) {
                        case DAY:
                            frag_end.add(Calendar.DAY_OF_MONTH, curr_interval); //sql表达式是 >= ... AND <= ，包含上下界值，所以间隔-1
                            break;
                        case MONTH:
                            frag_end.add(Calendar.MONTH, curr_interval);
                            break;
                        case YEAR:
                            frag_end.add(Calendar.YEAR, curr_interval);
                            break;
                    }
                    if (frag_end.after(c_end)) frag_end = (Calendar) c_end.clone();

                    //分片元数据
                    //将日期转换为毫秒单位整数
                    byte[] ms_start = JdbcUtil.getBytes(frag_start.getTimeInMillis());
                    byte[] ms_end = JdbcUtil.getBytes(frag_end.getTimeInMillis());
                    byte[] fragmentMetadata = JdbcUtil.mergeBytes(ms_start, ms_end);

                    byte[] userData = new byte[0];
                    Fragment fragment = new Fragment(inputData.getDataSource(), null, fragmentMetadata, userData);
                    fragments.add(fragment);

                    //继续下一个分片
                    frag_start = frag_end;
                    //frag_start.add(Calendar.DAY_OF_MONTH, 1);
                }
                break;
            }
            case INT: {
                int i_start = Integer.parseInt(range[0]);
                int i_end = Integer.parseInt(range[1]);
                int curr_interval = intervalVal;
                int curr_frags = (i_end - i_start) / curr_interval;
                if (curr_frags > MAX_FRAGMENTS) curr_interval = (i_end - i_start) / MAX_FRAGMENTS;
                //校验:curr_interval > 0
                int frag_start = i_start;
                while (frag_start < i_end) {
                    int frag_end = frag_start + curr_interval;
                    if (frag_end > i_end) frag_end = i_end;

                    byte[] b_start = JdbcUtil.getBytes(frag_start);
                    byte[] b_end = JdbcUtil.getBytes(frag_end);
                    byte[] fragmentMetadata = JdbcUtil.mergeBytes(b_start, b_end);

                    byte[] userData = new byte[0];
                    Fragment fragment = new Fragment(inputData.getDataSource(), null, fragmentMetadata, userData);
                    fragments.add(fragment);

                    //继续下一个分片
                    frag_start = frag_end;// + 1;
                }
                break;
            }
            case ENUM:
                for (String frag : range) {
                    byte[] fragmentMetadata = frag.getBytes();
                    Fragment fragment = new Fragment(inputData.getDataSource(), null, fragmentMetadata, new byte[0]);
                    fragments.add(fragment);
                }
                break;
        }

        return assignHost(fragments);
    }

    @Override
    public String buildFragmenterSql(String db_product, String origin_sql) {
        byte[] meta = inputData.getFragmentMetadata();
        if (meta == null)
            return origin_sql;

        StringBuilder sb = new StringBuilder(origin_sql);
        if (!origin_sql.contains("WHERE"))
            sb.append(" WHERE 1=1 ");

        sb.append(" AND ");
        switch (partitionType) {
            case DATE: {
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                //解析分片元数据
                //校验：meta长度==16
                byte[][] newb = JdbcUtil.splitBytes(meta, 8);
                Date frag_start = new Date(JdbcUtil.toLong(newb[0]));
                Date frag_end = new Date(JdbcUtil.toLong(newb[1]));

                if (db_product.toUpperCase().contains("MYSQL")) {
                    sb.append(partitionColumn).append(" >= DATE('").append(df.format(frag_start)).append("')");
                    sb.append(" AND ");
                    sb.append(partitionColumn).append(" < DATE('").append(df.format(frag_end)).append("')");
                } else if (db_product.toUpperCase().contains("ORACLE")) {
                    sb.append(partitionColumn).append(" >= to_date('").append(df.format(frag_start)).append("','yyyy-mm-dd')");
                    sb.append(" AND ");
                    sb.append(partitionColumn).append(" < to_date('").append(df.format(frag_end)).append("','yyyy-mm-dd')");
                } else
                    throw new UnsupportedTypeException("Unkwon Database Product: " + db_product);
                break;
            }
            case INT: {
                //解析分片元数据
                //校验：meta长度==8
                byte[][] newb = JdbcUtil.splitBytes(meta, 4);
                int frag_start = JdbcUtil.toInt(newb[0]);
                int frag_end = JdbcUtil.toInt(newb[1]);
                sb.append(partitionColumn).append(" >= ").append(frag_start);
                sb.append(" AND ");
                sb.append(partitionColumn).append(" < ").append(frag_end);
                break;
            }
            case ENUM:
                //解析分片元数据
                sb.append(partitionColumn).append("='").append(new String(meta)).append("'");
                break;
        }
        return sb.toString();
    }
}
