package com.insight.pxf;

import java.util.LinkedList;
import java.util.List;

import org.apache.hawq.pxf.api.FilterParser;
import org.apache.hawq.pxf.api.utilities.InputData;

public class MyDemoFilterBuilder implements FilterParser.FilterBuilder
{
    private InputData inputData;

    public MyDemoFilterBuilder(){}
    public MyDemoFilterBuilder(InputData input)
    {
        inputData = input;
    }

    /*
     * Translates a filterString into a FilterParser.BasicFilter or a list of such filters
     */
    public Object getFilterObject(String filterString) throws Exception
    {
        FilterParser parser = new FilterParser(this);
        Object result = parser.parse(filterString);

        if (!(result instanceof FilterParser.BasicFilter) && !(result instanceof List))
            throw new Exception("String " + filterString + " resolved to no filter");

        return result;
    }

    @Override
    public Object build(FilterParser.Operation opId,
                        Object leftOperand,
                        Object rightOperand) throws Exception
    {
        if (leftOperand instanceof FilterParser.BasicFilter)
        {
            //sanity check
            if (opId != FilterParser.Operation.HDOP_AND || !(rightOperand instanceof FilterParser.BasicFilter))
                throw new Exception("Only AND is allowed between compound expressions");

            //case 3
            if (leftOperand instanceof List)
                return handleCompoundOperations((List<FilterParser.BasicFilter>)leftOperand, (FilterParser.BasicFilter)rightOperand);
                //case 2
            else
                return handleCompoundOperations((FilterParser.BasicFilter)leftOperand, (FilterParser.BasicFilter)rightOperand);
        }

        //sanity check
        if (!(rightOperand instanceof FilterParser.Constant))
            throw new Exception("expressions of column-op-column are not supported");

        //case 1 (assume column is on the left)
        return handleSimpleOperations(opId, (FilterParser.ColumnIndex)leftOperand, (FilterParser.Constant)rightOperand);
    }

    private FilterParser.BasicFilter handleSimpleOperations(FilterParser.Operation opId,
                                                            FilterParser.ColumnIndex column,
                                                            FilterParser.Constant constant)
    {
        return new FilterParser.BasicFilter(opId, column, constant);
    }

    private  List handleCompoundOperations(List<FilterParser.BasicFilter> left,
                                           FilterParser.BasicFilter right)
    {
        left.add(right);
        return left;
    }

    private List handleCompoundOperations(FilterParser.BasicFilter left,
                                          FilterParser.BasicFilter right)
    {
        List<FilterParser.BasicFilter> result = new LinkedList<FilterParser.BasicFilter>();

        result.add(left);
        result.add(right);
        return result;
    }
}