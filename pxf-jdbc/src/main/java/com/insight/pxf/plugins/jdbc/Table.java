package com.insight.pxf.plugins.jdbc;

import com.insight.pxf.plugins.jdbc.utils.JdbcUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;

import java.io.IOException;
import java.net.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Enumeration;

/**
 * Created by jiadx on 2016/5/13.
 */
public class Table {
    private static final Log LOG = LogFactory.getLog(Table.class);
    private String jdbcDriver = null;
    private String dbUrl = null;
    private String user = null;
    private String pass = null;
    private String tblName = null;
    private int batchSize = 100;
    private int numAdded = 0;

    private String sql = null;
    private String whereSql = null;
    private Statement statement = null;
    private Connection conn = null;
    private InputData inputData = null;

    private String dbProduct = null;//数据库类型，从connection元数据获取。
    private ColumnDescriptor keyColumn = null;

    public Table(InputData input) throws Exception {
        this.inputData = input;
        jdbcDriver = input.getUserProperty("JDBC_DRIVER");
        dbUrl = input.getUserProperty("DB_URL");
        //dbUrl = "jdbc:mysql://192.168.200.6:3306/demodb";
        user = input.getUserProperty("USER");
        pass = input.getUserProperty("PASS");
        String strBatch = input.getUserProperty("BATCH_SIZE");
        if (strBatch != null) {
            batchSize = Integer.parseInt(strBatch);
        }

        if (jdbcDriver == null) {
            throw new IllegalArgumentException("JDBC_DRIVER must be set");
        } else if (dbUrl == null) {
            throw new IllegalArgumentException("DB_URL must be set(read)");
        } else {
            tblName = input.getDataSource();
            if (tblName == null) {
                throw new IllegalArgumentException("TABLE_NAME must be set as DataSource.");
            } else {
                tblName = tblName.toUpperCase();
            }
        }

        //构建sql语句，包括select主语句和where子句
        //数据库列定义-与目标数据库表列对应，不映射
        ArrayList<ColumnDescriptor> columns = input.getTupleDescription();
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        for (int i = 0; i < columns.size(); i++) {
            ColumnDescriptor column = columns.get(i);
            if (column.isKeyColumn())
                keyColumn = column;
            if (i > 0) sb.append(",");
            sb.append(column.columnName());
        }
        sb.append(" FROM ").append(getTableName());
        sql = sb.toString();

        whereSql = getWhereSql(inputData);
    }

    public Table(String driver, String url, String user, String pass, String tblName) {
        this.jdbcDriver = driver;
        this.dbUrl = url;
        this.user = user;
        this.pass = pass;
        this.tblName = tblName;
    }

    public String getTableName() {
        return tblName;
    }


    //检查JDBC驱动，打开数据库连接，检查数据库表是否存在
    public void open() throws Exception {
        if (statement != null)
            return;
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Open JDBC: driver=%s,url=%s,user=%s,pass=%s,table=%s",
                    jdbcDriver, dbUrl, user, pass, tblName));
        }
        Class.forName(jdbcDriver);
        if (user != null) {
            conn = DriverManager.getConnection(dbUrl, user, pass);
        } else {
            conn = DriverManager.getConnection(dbUrl);
        }
        DatabaseMetaData meta = conn.getMetaData();
        dbProduct = meta.getDatabaseProductName();


        //conn.setAutoCommit(false);
        statement = conn.createStatement();

        //检查数据库表是否存在（也可以不检查，而是在实际查询时再处理）
        //ResultSet rs = statement.executeQuery("SELECT 1 FROM "+tblName);
        //rs.close();
    }

    public ResultSet executeQuery() throws Exception {
        String query = sql;

        //执行查询，准备读取数据
        if (whereSql != null) {
            query = query + " WHERE " + whereSql;
        }
        //statement = conn.createStatement();//已经在openTable中创建
        open();

        query = JdbcFragmenter.buildFragmenterSql(inputData, dbProduct, query);

        if (LOG.isDebugEnabled()) {
            LOG.debug("executeQuery: " + query);
        }

        return statement.executeQuery(query);
    }

    //计算当前查询表的总行数
    public int getCount() throws Exception {
        String query = "SELECT count(1) FROM " + getTableName();
        if (whereSql != null) {
            query = query + " WHERE " + whereSql;
        }
        open();
        ResultSet rs = statement.executeQuery(query);
        try {
            if (rs.next())
                return rs.getInt(1);
        } finally {
            rs.close();
        }
        return 0;
    }

    /*
    对数据表查询进行分片，返回List数组。
    数组元素：分片起始行，每片行数
     */
    //public

    public static String getWhereSql(InputData inputData) throws Exception {
        if (inputData.hasFilter()) {
            String filterStr = inputData.getFilterString();
            //LOG.debug("openForRead()-->filterStr=" + filterStr);
            JdbcFilterBuilder eval = new JdbcFilterBuilder(inputData);
            return eval.getWhere(filterStr);
        }
        return null;
    }

    public void close() throws SQLException {
        if (statement != null && !statement.isClosed()) {
            statement.close();
            statement = null;
        }

        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    /*该数据库表所在的主机列表，用于并行化
    实现：
       1.初级：分析jdbc url- jdbc:mysql://192.168.200.6:3306/mysql&&USER=root&&PASS=root
    返回主机名。
       2.高级：检查数据库表分区等设置，返回信息
    注：返回的主机名应该是需要运行PXF服务，因为master是根据这个主机名分配segment，并让segment连接这个主机的PXF服务。
    */
    public String[] getHosts(String[] pxfmembers) {
        return pxfmembers;
        //return parseHosts(dbUrl);
    }

    public static String[] parseHosts(String dbUrl) {
        int start = dbUrl.indexOf("//");
        int end = dbUrl.indexOf("/", start + 2);
        String hostport = dbUrl.substring(start + 2, end);
        return new String[]{hostport};//.split(":")[0]};
    }
}
