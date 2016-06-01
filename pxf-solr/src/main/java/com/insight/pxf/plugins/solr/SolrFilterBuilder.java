package com.insight.pxf.plugins.solr;

import com.insight.pxf.plugins.SimpleFilterBuilder;
import org.apache.hawq.pxf.api.FilterParser;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.io.DataType;

import java.util.List;

/**
 * Created by jiadx on 2016/5/17.
 * 实现Solr的Query Filter Push-Down。
 */
public class SolrFilterBuilder extends SimpleFilterBuilder {
    private InputData inputData;

    public SolrFilterBuilder(InputData input) {
        inputData = input;
    }

    //根据查询条件构建查询语句
    public String buildQuery(String filterString) throws Exception {
        List<FilterParser.BasicFilter> filters = (List<FilterParser.BasicFilter>) getFilterObject(filterString);
        StringBuffer sb = new StringBuffer("");
        for (FilterParser.BasicFilter filter : filters) {
            if(sb.length() > 0) sb.append(" AND ");

            ColumnDescriptor column = inputData.getColumn(filter.getColumn().index());
            //列名
            sb.append(column.columnName()).append(":");

            //比较符
            String right = null; //用于范围查询的右封闭符号
            FilterParser.Operation op = filter.getOperation();
            switch (op) {
                case HDOP_GT:
                    sb.append("{ "); //op > 1 -> op:{1 TO *}
                    right = " TO *}";
                    break;
                case HDOP_LT:
                    sb.append("{* TO "); //op < 1 -> op:{* TO 1}
                    right = " }";
                    break;
                case HDOP_GE:
                    sb.append("[ "); //op >= 1 -> op:[1 TO *]
                    right = " TO *]";
                    break;
                case HDOP_LE:
                    sb.append("[* TO "); //op <= 1 -> op:[* TO 1]
                    right = " ]";
                    break;
                case HDOP_EQ:
                    //sb.append("");
                    right = "";
                    break;
                case HDOP_LIKE: //LIKE
                    //sb.append("");
                    right = "";
            }

            //表达式
            Object val = filter.getConstant().constant();
            switch (DataType.get(column.columnTypeCode())) {
                case INTEGER:
                case SMALLINT:
                case BIGINT:
                case FLOAT8:
                case REAL:
                case NUMERIC:
                case BOOLEAN:
                case VARCHAR:
                case TEXT:
                    sb.append(val.toString());
                    sb.append(right);
                    break;
                case DATE://solr查询日期型条件的格式：cdate:2008-01-01T00:00:00Z
                    sb.append(val.toString().trim());
                    sb.append("T00:00:00Z");
                    sb.append(right);
                    break;
                default:
                    throw new Exception("unsupported column type for filtering " + column.columnTypeCode());
            }
        }
        return sb.toString();
    }

}