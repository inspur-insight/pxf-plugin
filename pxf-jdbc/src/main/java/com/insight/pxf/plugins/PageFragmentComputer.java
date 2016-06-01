package com.insight.pxf.plugins;

import com.insight.pxf.plugins.jdbc.utils.JdbcUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hawq.pxf.api.Fragment;
import org.apache.hawq.pxf.api.utilities.InputData;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by jiadx on 2016/6/1.
 * 用于按照分页进行分片的解析计算.
 * 可用于JDBC的分页模式、和Solr。
 *
 * 客户端可通过FRAGMENT_ROWS参数设置参数来限制每个分片的最大处理数据行数，如果不定义则使用默认值
 */
public class PageFragmentComputer {
    //参数可以从pxf-site.xml中获取
    static int FRAGMENT_ROWS = 1000;//每个分片的最大行数
    static {
        Configuration config = new Configuration();
        config.addResource("pxf-site.xml");
        FRAGMENT_ROWS = config.getInt("com.insight.pxf.fragment_rows",1000);
    }

    InputData inputData = null;
    int page_rows = FRAGMENT_ROWS;

    public PageFragmentComputer(InputData inputData ){
        this.inputData = inputData;
        if(inputData.getUserProperty("FRAGMENT_ROWS") != null){
            page_rows = Integer.parseInt(inputData.getUserProperty("FRAGMENT_ROWS"));
        }

        ParamsChecker.assertTrue("MAX_ROWS must > 0", page_rows > 0);
    }

    public List<Fragment> computeFragments(int total_count) throws Exception {
        List<Fragment> fragments = new LinkedList();
        try {
            List<int[]> pages = computeFragment(total_count,page_rows);
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
        }
        return fragments;
    }


    //计算分片数据，数组元素：[起始行,当前分片行数]
    public static List<int[]> computeFragment(int count, int page_rows) {
        int page_num = count / page_rows ;

        List<int[]> pages = new ArrayList<>(page_num + 1);
        for (int i = 0; i < page_num; i++) {
            int[] pg = new int[]{page_rows * i, page_rows};
            pages.add(pg);
        }
        if (page_num * page_rows < count) {
            int[] pg = new int[]{page_num * page_rows, count - (page_num * page_rows)};
            pages.add(pg);
        }

        return pages;
    }
}
