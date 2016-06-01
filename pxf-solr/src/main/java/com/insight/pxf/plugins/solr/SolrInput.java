package com.insight.pxf.plugins.solr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;

import java.io.IOException;

/**
 * Created by jiadx on 2016/5/31.
 * 封装对solr引擎的数据读取，主要用于SolrReadAccessor和SolrFragmenter共享。
 */
public class SolrInput {
    private static final Log LOG = LogFactory.getLog(SolrReadAccessor.class);
    private HttpSolrServer server = null;
    private InputData inputData = null;

    public SolrInput(InputData input) throws Exception {
        this.inputData = input;
        String url = input.getUserProperty("URL");
        server = new HttpSolrServer(url + "/" + input.getDataSource());

    }

    public SolrDocumentList query() throws Exception {
        ModifiableSolrParams params = getParams(inputData);
        //起止行-TODO 取自fragment
        SolrFragmenter fragment = new SolrFragmenter(inputData);
        fragment.buildPages(params);

        if (LOG.isDebugEnabled())
            LOG.debug("Query Clause : " + params.get("q"));
        QueryResponse response = server.query(params);
        return response.getResults();
    }

    public long getCount() throws Exception {
        ModifiableSolrParams params = getParams(inputData);
        params.set("start", 0);
        params.set("rows", 1);
        QueryResponse response = server.query(params);
        SolrDocumentList list = response.getResults();
        return list.getNumFound();
    }

    private ModifiableSolrParams getParams(InputData input) throws Exception {
        ModifiableSolrParams params = new ModifiableSolrParams();

        //查询字段
        StringBuffer sb = new StringBuffer();
        for (ColumnDescriptor col : input.getTupleDescription()) {
            if (sb.length() > 0) sb.append(",");
            sb.append(col.columnName());
        }
        params.set("fl", sb.toString());

        params.set("q.op", "and");

        //查询条件
        if (input.hasFilter()) {
            SolrFilterBuilder eval = new SolrFilterBuilder(input);
            params.set("q", eval.buildQuery(input.getFilterString()));
        } else
            params.set("q", "*:*");
        return params;
    }
}
