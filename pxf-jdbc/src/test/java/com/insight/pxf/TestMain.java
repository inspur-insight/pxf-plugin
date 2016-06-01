package com.insight.pxf;

import com.insight.pxf.plugins.PageFragmentComputer;
import com.insight.pxf.plugins.jdbc.JdbcFilterBuilder;
import com.insight.pxf.plugins.jdbc.JdbcPageFragmenter;
import com.insight.pxf.plugins.jdbc.Table;
import com.insight.pxf.plugins.jdbc.utils.JdbcUtil;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by jiadx on 2016/5/16.
 */
public class TestMain {

    public static void main(String[] a) throws Exception {
        //testFilter();
        //testBytes();
        List<int[]> pages = PageFragmentComputer.computeFragment(11,3);
        for (int[] pg : pages) {
            System.out.println("Page:" + pg[0] + ", count=" + pg[1]);
        }
    }

    private static void testBytes() {
        int start = 110;
        int n = 5;
        byte[] bytes = JdbcUtil.mergeBytes(JdbcUtil.getBytes(start), JdbcUtil.getBytes(n));
        byte[][] newb = JdbcUtil.splitBytes(bytes, 4);
        System.out.println("START = " + JdbcUtil.toInt(newb[0]));
        System.out.println("END = " + JdbcUtil.toInt(newb[1]));
    }

    private static void testFilter() throws Exception {
        String filterstr = "a2c1o5"; //sex=1
        filterstr = "a0c1o5a2c1o5o7"; //id=1 and sex=1;
        //filterstr = "a0c1o5a2c1o5o7"; //id=1 and sex=1;
        filterstr = "a0c1o5a2c1o5o7a2c0o2o7a0c0o2o7"; //id=1 and sex=1 and sex>0;
        JdbcFilterBuilder fb = new JdbcFilterBuilder(null);
        Object filter = fb.getFilterObject(filterstr);

        System.out.println("FilterObject:" + filter);
    }

    private static void testTable() throws SQLException {
        //System.out.println("hosts = " + parseHosts("jdbc:mysql://192.168.200.6:3306/mysql&&USER=root&&PASS=root")[0]);
        Table table = new Table("com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.200.6:3306/demodb?useUnicode=true&characterEncoding=UTF-8",
                "root", "root", "DEMODB.MYCLASS");
        try {
            table.open();
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            table.close();
        }
    }
}
