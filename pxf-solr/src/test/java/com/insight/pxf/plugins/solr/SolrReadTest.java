package com.insight.pxf.plugins.solr;

import com.insight.pxf.PxfUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hawq.pxf.api.Fragmenter;
import org.apache.hawq.pxf.api.ReadAccessor;
import org.apache.hawq.pxf.api.ReadResolver;
import org.apache.hawq.pxf.api.io.DataType;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jiadx on 2016/5/27.
 */
public class SolrReadTest extends PxfUnit {
    private static final Log logger = LogFactory.getLog(SolrReadTest.class);
    private static final String SCHEMA_NAME = "sales";

    private static List<Pair<String, DataType>> columnDefs = new ArrayList<Pair<String, DataType>>();
    private static List<Pair<String, String>> extraParams = new ArrayList<Pair<String, String>>();

    private static final String URL = "http://10.110.17.21:8985/solr";

    @Before
    public void setup() {
        columnDefs.add(new Pair<String, DataType>("id", DataType.INTEGER));
        columnDefs.add(new Pair<String, DataType>("cdate", DataType.DATE));
        columnDefs.add(new Pair<String, DataType>("amt", DataType.FLOAT8));
        columnDefs.add(new Pair<String, DataType>("grade", DataType.TEXT));
        columnDefs.add(new Pair<String, DataType>("score", DataType.REAL));
        // System.getenv().put("HADOOP_HOME","C:\\Java\\hadoop-2.6.1");

        extraParams.add(new Pair<String, String>("URL", URL));
        com.insight.pxf.plugins.solr.SolrFragmenter fragmenter = null;
    }

    @Test
    public void testSimpleQuery() throws Exception {
        super.setup(SCHEMA_NAME, null);
        List<String> result = getAllOutput();
        for (String str : result) {
            System.out.println("result =>" + str);
        }
    }

    @Test
    public void testFragment() throws Exception {
        extraParams.add(new Pair<String, String>("MAX_ROWS", "100"));
        extraParams.add(new Pair<String, String>("FRAGMENT_ROWS", "4"));

        String filterString = null;
        assertFragmentsOutput(SCHEMA_NAME, filterString, 16);
    }

    @Test
    public void testDateFilter() throws Exception {
        extraParams.add(new Pair<String, String>("MAX_ROWS", "100"));
        extraParams.add(new Pair<String, String>("FRAGMENT_ROWS", "4"));

        String filterString = "a1c\"2008-10-01\"o4";//cdate >= '2008-10-01'
        assertFragmentsOutput(SCHEMA_NAME, filterString, 7);
    }
    @Test
    public void testIdFilter() throws Exception{
        String filterString = "a0c5o4";//id>=5
        assertFragmentsOutput(SCHEMA_NAME, filterString, 12);
    }


    @Override
    public Class<? extends ReadAccessor> getReadAccessorClass() {
        return SolrReadAccessor.class;
    }

    @Override
    public Class<? extends ReadResolver> getReadResolverClass() {
        return SolrReadResolver.class;
    }

    @Override
    public List<Pair<String, String>> getExtraParams() {
        return extraParams;
    }

    @Override
    public List<Pair<String, DataType>> getColumnDefinitions() {
        return columnDefs;
    }

    @Override
    public Class<? extends Fragmenter> getFragmenterClass() {
        return SolrFragmenter.class;
    }

}
