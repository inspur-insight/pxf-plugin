package com.insight.pxf.plugins.jdbc;

import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadAccessor;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;

public class JdbcReadAccessor extends Plugin implements ReadAccessor {
    private static final Log LOG = LogFactory.getLog(JdbcReadAccessor.class);

    private Table table = null;

    //数据库列定义-与目标数据库表列对应，不映射
    //private ArrayList<ColumnDescriptor> columns = null;

    //private String sql = null;
    private ResultSet resultSet = null;

    public JdbcReadAccessor(InputData input) throws Exception {
        super(input);
        table = new Table(input);
    }

    @Override
    public boolean openForRead() throws Exception {
        table.open();

        resultSet = table.executeQuery();

        return true;
    }

    @Override
    public OneRow readNextObject() throws Exception {
        if (resultSet.next()) {
            return new OneRow(null, resultSet);
        }
        return null;
    }

    @Override
    public void closeForRead() throws Exception {
        table.close();
    }
}