package com.insight.pxf.plugins;

import org.apache.hawq.pxf.api.FilterParser;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by jiadx on 2016/5/31.
 */
public class SimpleFilterBuilder implements FilterParser.FilterBuilder  {

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

    //2个检查实际可以不执行，因为不符合这2种情况的条件也不会pushdown到PXF
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
