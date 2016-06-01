package com.insight.pxf.plugins.solr;

import com.insight.pxf.plugins.PageFragmentComputer;
import com.insight.pxf.plugins.ParamsChecker;
import com.insight.pxf.plugins.jdbc.JdbcFragmenterFactory;
import com.insight.pxf.plugins.jdbc.utils.JdbcUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hawq.pxf.api.Fragment;
import org.apache.hawq.pxf.api.Fragmenter;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.solr.common.params.ModifiableSolrParams;

import java.util.List;

/**
 * Created by jiadx on 2016/5/26.
 */
public class SolrFragmenter extends Fragmenter {
    static float MIN_SCORE = (float) 0.5;//文档的最低评分--意义不大
    static int MAX_ROWS = 1000;//查询的最大行数
    static {
        Configuration config = new Configuration();
        config.addResource("pxf-site.xml");
        MAX_ROWS = config.getInt("com.insight.pxf.solr.max_rows",10000);
    }

    PageFragmentComputer page_computer = null;
    //外部表定义的参数
    //float min_score = MIN_SCORE;
    int max_rows = MAX_ROWS;

    public SolrFragmenter(InputData inputData) {
        super(inputData);
        page_computer = new PageFragmentComputer(inputData);
        if (inputData.getUserProperty("MAX_ROWS") != null)
            max_rows = Integer.parseInt(inputData.getUserProperty("MAX_ROWS"));
        ParamsChecker.assertTrue("MAX_ROWS must > 0", max_rows > 0 );
    }

    @Override
    public List<Fragment> getFragments() throws Exception {
        SolrInput solrinput = new SolrInput(inputData);
        long total_count = solrinput.getCount();
        if (total_count > max_rows) total_count = max_rows;

        ParamsChecker.assertTrue("Solr Query Result.numFound  must < Integer.MAX", ((int)total_count) == total_count );//long转换int时可能负值

        List<Fragment> pages = page_computer.computeFragments((int)total_count);
        fragments.addAll(pages);

        fragments = JdbcFragmenterFactory.assignHost(fragments);
        return fragments;
    }

    public void buildPages(ModifiableSolrParams params) {
        //分片查询信息：当前页起始行、每页行数
        int page_start = 0;
        int page_num = max_rows;
        if(inputData.getFragmentMetadata() != null){

            //解析分片元数据
            byte[] meta = inputData.getFragmentMetadata();
            if(meta != null && meta.length == 8) {
                byte[][] newb = JdbcUtil.splitBytes(meta, 4);
                page_start = JdbcUtil.toInt(newb[0]);
                page_num = JdbcUtil.toInt(newb[1]);
            }

        }
        params.set("start", page_start);
        params.set("rows", page_num);
    }
}
