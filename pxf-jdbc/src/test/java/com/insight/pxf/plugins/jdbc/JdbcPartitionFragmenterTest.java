package com.insight.pxf.plugins.jdbc;

import org.apache.hawq.pxf.api.*;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.*;

public class JdbcPartitionFragmenterTest extends JdbcUnit {
    private static List<Pair<String, DataType>> columnDefs = new ArrayList<Pair<String, DataType>>();
    private static List<Pair<String, String>> extraParams = new ArrayList<Pair<String, String>>();

    @Before
    public void setup() throws Exception {
        extraParams.add(new Pair<String, String>("FRAGMENTER", "com.insight.pxf.plugins.jdbc.JdbcPartitionFragmenter"));
    }

    @After
    public void cleanup() throws Exception {
        extraParams.clear();
    }

    @Test
    public void testDateFrags() throws Exception {
        //PARTITION_BY=createdate:date&RANGE=2008-01-01:2010-01-01&INTERVAL=1:day
        extraParams.add(new Pair<String, String>("PARTITION_BY", "cdate:date"));
        extraParams.add(new Pair<String, String>("RANGE", "2008-01-01:2010-01-01"));
        extraParams.add(new Pair<String, String>("INTERVAL", "1:month"));

        super.setup("sales", null);

        for (InputData input : frag_inputs){
            JdbcFragmenter fragment = (JdbcFragmenter) getFragmenter(input);
            System.out.println("Frag SQL : " + fragment.buildFragmenterSql("mysql", "select * from sales"));
        }
    }

    @Test
    public void testIntFrags() throws Exception {
        //PARTITION_BY=year:int&RANGE=2008:2010&INTERVAL=1
        extraParams.add(new Pair<String, String>("PARTITION_BY", "id:int"));
        extraParams.add(new Pair<String, String>("RANGE", "1:3"));
        extraParams.add(new Pair<String, String>("INTERVAL", "11"));

        super.setup("sales", null);

        for (InputData input : frag_inputs){
            JdbcFragmenter fragment = (JdbcFragmenter) getFragmenter(input);
            System.out.println("Frag SQL : " + fragment.buildFragmenterSql("mysql", "select * from sales"));
        }
    }

    @Test
    public void testEnumFrags() throws Exception {
        //PARTITION_BY=level:enum&RANGE=excellent:good:general:bad
        extraParams.add(new Pair<String, String>("PARTITION_BY", "grade:enum"));
        extraParams.add(new Pair<String, String>("RANGE", "excellent:good:general:bad"));

        super.setup("sales", null);

        for (InputData input : frag_inputs){
            JdbcFragmenter fragment = (JdbcFragmenter) getFragmenter(input);
            System.out.println("Frag SQL : " + fragment.buildFragmenterSql("mysql", "select * from sales"));
        }
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
        return JdbcPartitionFragmenter.class;
    }
}