package com.insight.pxf.plugins.solr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadResolver;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;
import org.apache.solr.common.SolrDocument;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by jiadx on 2016/5/26.
 */
public class SolrReadResolver extends Plugin implements ReadResolver {
    private static final Log LOG = LogFactory.getLog(SolrReadResolver.class);
    //HAWQ数据库列定义-
    private ArrayList<ColumnDescriptor> columns = null;

    public SolrReadResolver(InputData input) {
        super(input);
        columns = input.getTupleDescription();
    }

    @Override
    public List<OneField> getFields(OneRow oneRow) throws Exception {
        SolrDocument doc = (SolrDocument) oneRow.getData();
        LinkedList<OneField> fields = new LinkedList<OneField>();

        for (int i = 0; i < columns.size(); i++) {
            ColumnDescriptor column = columns.get(i);
            String colname = column.columnName();
            Object value = null;//result.getObject(column.columnName());

            OneField oneField = new OneField();
            oneField.type = column.columnTypeCode();
            switch (DataType.get(oneField.type)) {
                case INTEGER:
                case SMALLINT:
                    value = (Integer)doc.get(colname);
                    break;
                case BIGINT:
                    value = (Long)doc.get(colname);
                    break;
                case FLOAT8:
                    value = (Double)doc.get(colname);
                    break;
                case REAL:
                    value = (Float)doc.get(colname);
                    break;
                case NUMERIC:
                    value = (Double)doc.get(colname);//??
                    break;
                case BOOLEAN:
                    value = (Boolean)doc.get(colname);
                    break;
                case VARCHAR:
                case TEXT:
                    value = (String)doc.get(colname);
                    break;
                case DATE:
                    //gpdbOutput.setString(colIdx,ObjectUtils.toString(val, null));
                    value = (Date)doc.get(colname);
                    break;
                default:
                    throw new UnsupportedOperationException("Unknwon Field Type : " + DataType.get(oneField.type).toString()
                            +", Column : " + column.toString());
            }
            //oneField.val = convertToJavaObject(oneField.type, column.columnTypeName(), value);
            oneField.val = value;
            fields.add(oneField);
        }
        return fields;
    }
}
