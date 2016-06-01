package com.insight.pxf.plugins.solr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadAccessor;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;

/**
 * Created by jiadx on 2016/5/26.
 * Solr外部读取类
 *
 */
public class SolrReadAccessor extends Plugin implements ReadAccessor  {
    private static final Log LOG = LogFactory.getLog(SolrReadAccessor.class);

    private HttpSolrServer server = null;
    ModifiableSolrParams params = new ModifiableSolrParams();

    //查询结果
    SolrDocumentList query_result = null;
    int cursor = 0; //游标

    public SolrReadAccessor(InputData input) throws Exception {
        super(input);
        String url = input.getUserProperty("URL");
        server = new HttpSolrServer(url+"/"+input.getDataSource());

        //查询字段
        StringBuffer sb = new StringBuffer();
        for(ColumnDescriptor col: input.getTupleDescription()){
            if(sb.length()>0) sb.append(",");
            sb.append(col.columnName());
        }
        params.set("fl",sb.toString());

        params.set("q.op", "and");
        //起止行-TODO 取自fragment
        SolrFragmenter fragment = new SolrFragmenter(input);
        fragment.buildPages(params);
        //params.set("start", 0);
        //params.set("rows", 5); //默认10行，

        //查询条件
        if(input.hasFilter()){
            SolrFilterBuilder eval = new SolrFilterBuilder(inputData);
            params.set("q", eval.buildQuery(input.getFilterString()));
        }else
            params.set("q", "*:*");

        //设置评分条件
    }

    @Override
    public boolean openForRead() throws Exception {
        if(LOG.isDebugEnabled())
            LOG.debug("Query Clause : " + params.get("q"));
        QueryResponse response = server.query(params);
        query_result = response.getResults();
        cursor = 0;
        return true;
    }

    @Override
    public OneRow readNextObject() throws Exception {
        if(cursor >= query_result.size())
            return null;
        return new OneRow(null,query_result.get(cursor++));
    }

    @Override
    public void closeForRead() throws Exception {
        query_result.clear();
        cursor = 0;
        server.close();
    }
}
