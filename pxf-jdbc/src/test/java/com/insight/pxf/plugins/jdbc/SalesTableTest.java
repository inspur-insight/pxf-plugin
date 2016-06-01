package com.insight.pxf.plugins.jdbc;

import org.apache.hawq.pxf.api.Fragmenter;
import org.apache.hawq.pxf.api.io.DataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SalesTableTest extends JdbcUnit {
    private static final String TABLE_NAME = "sales";

    private static List<Pair<String, DataType>> columnDefs = new ArrayList<Pair<String, DataType>>();
    private static List<Pair<String, String>> extraParams = new ArrayList<Pair<String, String>>();

    private Class framenterClass = JdbcFragmenterFactory.class;


    @Before
    public void setup() throws Exception {
        //extraParams.add(new Pair<String, String>("FRAGMENTER", "com.insight.pxf.plugins.jdbc.JdbcPartitionFragmenter"));
        columnDefs.add(new Pair<String, DataType>("ID",DataType.INTEGER));
        columnDefs.add(new Pair<String, DataType>("CDATE",DataType.DATE));
        columnDefs.add(new Pair<String, DataType>("AMT",DataType.FLOAT8));
        columnDefs.add(new Pair<String, DataType>("grade",DataType.TEXT));
       // System.getenv().put("HADOOP_HOME","C:\\Java\\hadoop-2.6.1");
        JdbcParamsUtil.buildOracleDriver(extraParams);
    }

    @After
    public void cleanup() throws Exception {
        extraParams.clear();
        columnDefs.clear();
        super.cleanup();
    }

    @Test
    public void testDateFrags() throws Exception {
        //PARTITION_BY=createdate:date&RANGE=2008-01-01:2010-01-01&INTERVAL=1:day
        extraParams.add(new Pair<String, String>("PARTITION_BY", "cdate:date"));
        extraParams.add(new Pair<String, String>("RANGE", "2008-01-01:2010-01-01"));
        extraParams.add(new Pair<String, String>("INTERVAL", "1:month"));

        assertFragmentsOutput(TABLE_NAME, null, 16);
    }

    @Test
    public void testIntFrags() throws Exception {
        //PARTITION_BY=year:int&RANGE=2008:2010&INTERVAL=1
        extraParams.add(new Pair<String, String>("PARTITION_BY", "id:int"));
        extraParams.add(new Pair<String, String>("RANGE", "1:30"));
        extraParams.add(new Pair<String, String>("INTERVAL", "7"));

        assertFragmentsOutput(TABLE_NAME, null, 16);
    }

    @Test
    public void testEnumFrags() throws Exception {
        //PARTITION_BY=level:enum&RANGE=excellent:good:general:bad
        extraParams.add(new Pair<String, String>("PARTITION_BY", "grade:enum"));
        extraParams.add(new Pair<String, String>("RANGE", "excellent:good:general:bad"));

        assertFragmentsOutput(TABLE_NAME, null, 16);
    }

    @Test
    public void testPageFrags() throws Exception {
        assertFragmentsOutput(TABLE_NAME, null, 16);
    }

    @Test
    public void testIntFilter() throws Exception {
        String filterString = "a0c5o4";//id>=5
        assertFragmentsOutput(TABLE_NAME, filterString, 12);
    }

    @Test
    public void testDateFilter() throws Exception {
        String filterString = "a1c\"2008-10-01\"o4";//cdate >= '2008-10-01'//暂不支持
        assertFragmentsOutput(TABLE_NAME, filterString, 7);
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
        return framenterClass;
    }
}