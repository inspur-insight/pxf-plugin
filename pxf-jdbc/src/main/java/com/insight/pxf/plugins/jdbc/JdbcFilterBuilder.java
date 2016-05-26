package com.insight.pxf.plugins.jdbc;


import java.util.LinkedList;
import java.util.List;

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
public class JdbcFilterBuilder implements FilterParser.FilterBuilder {
    private InputData inputData;

    public JdbcFilterBuilder(InputData input) {
        inputData = input;
    }

    public String getWhere(String filterString) throws Exception {
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
            Object val = filter.getConstant().constant();
            switch (DataType.get(column.columnTypeCode())) {
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                    sb.append(val.toString());
                    break;
                case TEXT:
                    sb.append('"').append(val.toString()).append('"');
                    break;
                default:
                    throw new Exception("unsupported column type for filtering " + column.columnTypeCode());
            }

            sb.append("");
        }
        return sb.toString();
    }

    /*
     * Translates a filterString into a FilterParser.BasicFilter or a list of such filters
     */
    public Object getFilterObject(String filterString) throws Exception {
        FilterParser parser = new FilterParser(this);
        Object result = parser.parse(filterString);

        if (!(result instanceof FilterParser.BasicFilter) && !(result instanceof List))
            throw new Exception("String " + filterString + " resolved to no filter");

        if (!(result instanceof List)) {
            LinkedList<FilterParser.BasicFilter> list = new LinkedList<FilterParser.BasicFilter>();
            list.add((FilterParser.BasicFilter) result);
            result = list;
        }

        return result;
    }

    //2个检查实际可用不执行，因为不符合这2种情况的条件也不会pushdown到PXF
    /*
      filter构建过程：
       1.从左到右解析
       2.解析字段的表达式条件，构建出BasicFilter对象
       3.如果下一个操作是组合（AND），则先解析右边的表达式
       4.根据AND将多个条件进行组合，创建BasicFilter的集合->做为下一个组合的leftOperand
       5.继续3
     */
    @Override
    public Object build(FilterParser.Operation opId,
                        Object leftOperand,
                        Object rightOperand) throws Exception {
        if (leftOperand instanceof FilterParser.BasicFilter
                || leftOperand instanceof List) {
            //检查1：多个条件之间只能是AND关系
            if (opId != FilterParser.Operation.HDOP_AND || !(rightOperand instanceof FilterParser.BasicFilter))
                throw new Exception("Only AND is allowed between compound expressions");

            //case 3
            if (leftOperand instanceof List)
                return handleCompoundOperations((List<FilterParser.BasicFilter>) leftOperand, (FilterParser.BasicFilter) rightOperand);
                //case 2
            else
                return handleCompoundOperations((FilterParser.BasicFilter) leftOperand, (FilterParser.BasicFilter) rightOperand);
        }

        //检查2：条件表达式右边必须是常量
        if (!(rightOperand instanceof FilterParser.Constant))
            throw new Exception("expressions of column-op-column are not supported");

        //case 1 (assume column is on the left)
        return handleSimpleOperations(opId, (FilterParser.ColumnIndex) leftOperand, (FilterParser.Constant) rightOperand);
    }

    private FilterParser.BasicFilter handleSimpleOperations(FilterParser.Operation opId,
                                                            FilterParser.ColumnIndex column,
                                                            FilterParser.Constant constant) {
        return new FilterParser.BasicFilter(opId, column, constant);
    }

    private List handleCompoundOperations(List<FilterParser.BasicFilter> left,
                                          FilterParser.BasicFilter right) {
        left.add(right);
        return left;
    }

    private List handleCompoundOperations(FilterParser.BasicFilter left,
                                          FilterParser.BasicFilter right) {
        List<FilterParser.BasicFilter> result = new LinkedList<FilterParser.BasicFilter>();

        result.add(left);
        result.add(right);
        return result;
    }
}
