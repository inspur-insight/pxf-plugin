package com.insight.pxf.plugins.jdbc;

import com.insight.pxf.PxfUnit;

import java.util.List;

/**
 * Created by jiadx on 2016/5/26.
 */
public class JdbcParamsUtil {

    public static void buildMysqlDriver(List<PxfUnit.Pair<String, String>> params) {
        String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        String DB_URL = "jdbc:mysql://10.110.17.21:3306/demodb";
        String USER = "root";
        String PASS = "root";
        
        params.add(new PxfUnit.Pair<String, String>("JDBC_DRIVER", JDBC_DRIVER));
        params.add(new PxfUnit.Pair<String, String>("DB_URL", DB_URL));
        params.add(new PxfUnit.Pair<String, String>("USER", USER));
        params.add(new PxfUnit.Pair<String, String>("PASS", PASS));

    }
    public static void buildOracleDriver(List<PxfUnit.Pair<String, String>> params) {
        String JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";
        String DB_URL = "jdbc:oracle:thin:@10.110.17.21:1521:xe";
        String USER = "sys as sysdba";
        String PASS = "oracle";

        params.add(new PxfUnit.Pair<String, String>("JDBC_DRIVER", JDBC_DRIVER));
        params.add(new PxfUnit.Pair<String, String>("DB_URL", DB_URL));
        params.add(new PxfUnit.Pair<String, String>("USER", USER));
        params.add(new PxfUnit.Pair<String, String>("PASS", PASS));

    }
}
