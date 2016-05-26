package com.insight.pxf.plugins.jdbc;
import java.io.IOException;
import java.sql.*;

import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.WriteAccessor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;

public class JdbcAccessor extends Plugin implements WriteAccessor {

    // private static final Logger LOG = Logger.getLogger(JdbcAccessor.class);
    private String jdbcDriver = null;
    private String dbUrl = null;
    private String user = null;
    private String pass = null;
    private String tblName = null;
    private Connection conn = null;
    private Statement statement = null;
    private int batchSize = 100;
    private int numAdded = 0;

    public JdbcAccessor(InputData input) throws IOException {
        super(input);
        this.jdbcDriver = input.getUserProperty("JDBC_DRIVER");
        this.dbUrl = input.getUserProperty("DB_URL");
        this.user = input.getUserProperty("USER");
        this.pass = input.getUserProperty("PASS");
        String strBatch = input.getUserProperty("BATCH_SIZE");
        if(strBatch != null) {
            this.batchSize = Integer.parseInt(strBatch);
        }

        if(this.jdbcDriver == null) {
            throw new IllegalArgumentException("JDBC_DRIVER must be set");
        } else if(this.dbUrl == null) {
            throw new IllegalArgumentException("DB_URL must be set");
        } else {
            this.tblName = input.getUserProperty("TABLE_NAME");
            if(this.tblName == null) {
                throw new IllegalArgumentException("TABLE_NAME must be set");
            } else {
                this.tblName = this.tblName.toUpperCase();
            }
        }
    }

    @Override
    public boolean openForWrite() throws Exception {
        Class.forName(this.jdbcDriver);
        if(this.user != null) {
            this.conn = DriverManager.getConnection(this.dbUrl, this.user, this.pass);
        } else {
            this.conn = DriverManager.getConnection(this.dbUrl);
        }

        this.conn.setAutoCommit(false);
        this.statement = this.conn.createStatement();
        if(this.tblName.contains(".")) {
            String tables = this.tblName.split("\\.")[0];
            String found = this.tblName.split("\\.")[1];
            this.statement.execute("USE " + tables);
            ResultSet tables1 = this.statement.executeQuery("SHOW TABLES");
            boolean found1 = false;

            while(tables1.next()) {
                if(tables1.getString(1).equals(found)) {
                    found1 = true;
                    break;
                }
            }

            if(!found1) {
                throw new SQLException("Table " + this.tblName + " not found.");
            }
        } else {
            ResultSet tables2 = this.statement.executeQuery("SHOW TABLES");
            boolean found2 = false;

            while(tables2.next()) {
                if(tables2.getString(1).equals(this.tblName)) {
                    found2 = true;
                    break;
                }
            }

            if(!found2) {
                throw new SQLException("Table " + this.tblName + " not found.");
            }
        }

        return true;
    }

    @Override
    public boolean writeNextObject(OneRow row) throws Exception {
        this.statement.addBatch(row.getData().toString());
        ++this.numAdded;
        if(this.numAdded % this.batchSize == 0) {
            this.statement.executeBatch();
            this.numAdded = 0;
        }

        return true;
    }

    @Override
    public void closeForWrite() throws Exception {
        if(this.statement != null && this.numAdded != 0) {
            this.statement.executeBatch();
            this.numAdded = 0;
        }

        if(this.conn != null) {
            this.conn.commit();
            this.conn.close();
        }
    }
}