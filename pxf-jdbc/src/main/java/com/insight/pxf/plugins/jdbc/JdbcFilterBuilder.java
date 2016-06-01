package com.insight.pxf.plugins.jdbc;


import java.util.List;

import com.insight.pxf.plugins.SimpleFilterBuilder;
import com.insight.pxf.plugins.jdbc.utils.DbProduct;
import com.insight.pxf.plugins.jdbc.utils.JdbcUtil;
import org.apache.hawq.pxf.api.FilterParser;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;

/**
 * Created by jiadx on 2016/5/17.
 * 实现Jdbc的Query Filter Push-Down。
 * 从测试结果发现，hawq只会把int型的比较语句下推到PXF。（更正：可支持int、TEXT，参见源码->src/backend/access/external.pxffilters.c）
 *
 hawq中建立的SQL表：
 CREATE EXTERNAL TABLE myclass(id integer,
 name varchar,
 sex integer,
 degree float8)
 LOCATION ('pxf://localhost:51200/demodb.myclass'
 '?PROFILE=JDBC'
 '&JDBC_DRIVER=com.mysql.jdbc.Driver'
 '&DB_URL=jdbc:mysql://192.168.200.6:3306/demodb&USER=root&PASS=root'
 )
 FORMAT 'CUSTOM' (Formatter='pxfwritable_import');
 * 则查询语句的下推如下：
 * 1.SQL: select * from myclass where sex=0;
 * filterString: a2c1o5
 * 2.select * from myclass where id=1 and sex=1;
 * filter: a0c1o5a2c1o5o7
 * 3.select * from myclass where sex=1 and degree>30;
 * filter: a2c1o5
 * degrees字段是浮点型，只push了"sex=1"的条件
 * 4.select * from myclass where name='tom' and sex=1;
 * filter: a2c1o5
 * 同样由于name是字符串类型，条件未下推
 * 5.select * from myclass where id=1 or sex=1;
 * filter:
 * OR条件未下推到PXF。
 * 6.select * from myclass where id=sex;
 * filter:
 * 条件未下推
 * 7.select * from myclass where id=1 and sex=1 and sex=0;
 * filter:
 * hawq已经解析了where条件是false，未下推。(实际的查询也未下推)
 * 关联查询：
 * a.关联查询： select * from my_table,myclass where my_table.first=myclass.id;
 * filter:
 * 没有条件下推。
 * b.关联：select * from my_table,myclass where my_table.first=myclass.id and sex=1
 * filter: a2c1o5
 * 下推条件: sex=1
 */
public class JdbcFilterBuilder extends SimpleFilterBuilder {
    private InputData inputData;

    public JdbcFilterBuilder(InputData input) {
        inputData = input;
    }

    public String buildWhereExpress(String db_product) throws Exception {
        if(!inputData.hasFilter()) return null;
        String filterString = inputData.getFilterString();

        List<FilterParser.BasicFilter> filters = (List<FilterParser.BasicFilter>) getFilterObject(filterString);
        StringBuffer sb = new StringBuffer("1=1");
        for (FilterParser.BasicFilter filter : filters) {
            sb.append(" AND ");

            ColumnDescriptor column = inputData.getColumn(filter.getColumn().index());
            //列名
            sb.append(column.columnName());

            //比较符
            FilterParser.Operation op = filter.getOperation();
            switch (op) {
                case HDOP_LT:
                    sb.append("<");
                    break;
                case HDOP_GT:
                    sb.append(">");
                    break;
                case HDOP_LE:
                    sb.append("<=");
                    break;
                case HDOP_GE:
                    sb.append(">=");
                    break;
                case HDOP_EQ:
                    sb.append("=");
                    break;
            }
            //表达式
            DbProduct dbProduct = JdbcUtil.getDbProduct(db_product);
            Object val = filter.getConstant().constant();
            switch (DataType.get(column.columnTypeCode())) {
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case FLOAT8:
                case REAL:
                case BOOLEAN:
                    sb.append(val.toString());
                    break;
                case TEXT:
                    sb.append('"').append(val.toString()).append('"');
                    break;
                case DATE:
                    //需要根据数据库定制
                    sb.append(dbProduct.wrapDate(val));
                    break;
                default:
                    throw new Exception("unsupported column type for filtering " + column.columnTypeCode());
            }

            sb.append("");
        }
        return sb.toString();
    }
}
