package com.insight.pxf.plugins.solr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.*;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.*;
import org.apache.solr.common.*;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by jiadx on 2016/5/27.
 */
public class SolrDemo {
    private static final Log logger = LogFactory.getLog(SolrDemo.class);

    private static final String URL = "http://10.110.17.21:8985/solr/sales";

    private HttpSolrServer server = null;

    @Before
    public void init() {
        // 创建 server
        server = new HttpSolrServer(URL);
    }

    /**
     * 查询
     */
    @Test
    public void testQuery() throws IOException, SolrServerException {
        String queryStr = "*:*";
        SolrQuery params = new SolrQuery(queryStr);
       // params.set("rows", 10);
        QueryResponse response = server.query(params);
        SolrDocumentList list = response.getResults();
        logger.info("########### 总共 ： " + list.getNumFound() + "条记录");
        for (SolrDocument doc : list) {
            logger.info("######### id : " + doc.get("id") + "  title : " + doc.get("title"));
        }
    }

    /**
     * 查询，只返回行数
     */
    @Test
    public void queryCount() throws IOException, SolrServerException {
        //list.getNumFound()
        String queryStr = "id:*";
        SolrQuery params = new SolrQuery(queryStr);
        params.set("rows", 1);
        QueryResponse response = server.query(params);
        SolrDocumentList list = response.getResults();
        logger.info("########### 总共 ： " + list.getNumFound() + "条记录");
    }

    /**
     * 简单查询(分页)
     */
    @Test
    public void querySimple() throws IOException, SolrServerException {
        ModifiableSolrParams params = new ModifiableSolrParams();
        //params.set("q", "id:1");
        //params.set("q", "id:[3 TO 10]");//>=3 AND <=10
        //params.set("q", "id:[3 TO *] AND id:{* TO 10} AND grade:good");//>=3 AND <10
        //params.set("q", "amt:[1000 TO 1300]");//>=1000 AND <=1200 //未索引不能执行
        //params.set("q", "cdate:[2008-01-01T00:00:00Z TO 2008-10-01T00:00:00Z]");//日期型
        //params.set("q", "cdate:[2008-01-01 TO 2008-10-01]");//日期型,Invalid Date String:'2008-01-01'
        params.set("q","grade:good");
        params.set("q.op", "and");
        params.set("start", 0);
        params.set("rows", 5);
        params.set("fl", "*,score");//返回所有字段+score
        params.set("maxscore","tie=1");
        QueryResponse response = server.query(params);
        SolrDocumentList list = response.getResults();
        logger.info("########### 总共 ： " + list.getNumFound() + "条记录");
        for (SolrDocument doc : list) {
            logger.info("######### id : " + doc.get("id") + "  date : "
                    + doc.get("cdate") + " ,score = " + doc.get("score"));
        }
    }

    /**
     * 查询(分页,高亮)
     */
    @Test
    public void queryCase() throws IOException, SolrServerException {
        String queryStr = "title:this";
        SolrQuery params = new SolrQuery(queryStr);
        params.set("start", 0);
        params.set("rows", 5);

        // 启用高亮组件, 设置高亮
        params.setHighlight(true)
                .addHighlightField("title")
                .setHighlightSimplePre("<span class=\"red\">")
                .setHighlightSimplePost("</span>")
                .setHighlightSnippets(2)
                .setHighlightFragsize(1000)
                .setStart(0)
                .setRows(10)
                .set("hl.useFastVectorHighlighter", "true")
                .set("hl.fragsize", "200");

        QueryResponse response = server.query(params);
        SolrDocumentList list = response.getResults();
        logger.info("########### 总共 ： " + list.getNumFound() + "条记录");
        for (SolrDocument doc : list) {
            logger.info("######### id : " + doc.get("id") + "  title : " + doc.get("title"));
        }

        Map<String, Map<String, List<String>>> map = response.getHighlighting();
        Iterator<String> iterator = map.keySet().iterator();

        while (iterator.hasNext()) {
            String key = iterator.next();
            Map<String, List<String>> values = map.get(key);
            logger.info("############################################################");
            logger.info("############ id : " + key);

            for (Map.Entry<String, List<String>> entry : values.entrySet()) {
                String subKey = entry.getKey();
                List<String> subValues = entry.getValue();

                logger.info("############ subKey : " + subKey);
                for (String str : subValues) {
                    logger.info("############ subValues : " + str);
                }
            }

        }
    }

}
