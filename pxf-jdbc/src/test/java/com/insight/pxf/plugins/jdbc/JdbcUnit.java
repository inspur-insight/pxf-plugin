package com.insight.pxf.plugins.jdbc;

import com.insight.pxf.PxfUnit;
import org.apache.hawq.pxf.api.ReadAccessor;
import org.apache.hawq.pxf.api.ReadResolver;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.junit.Assert;
import org.junit.Before;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * Created by jiadx on 2016/5/26.
 */
public abstract class JdbcUnit extends PxfUnit {


    //检查分区数据是否完整
    public void assertFragmentsOutput(String input,int expectedRows) throws Exception {
        setup(input);

        //查询全表数据
        ReadAccessor accessor = getReadAccessor(masterInputData);
        ReadResolver resolver = getReadResolver(masterInputData);
        List<String> expectedOutput = getAllOutput(accessor,resolver);
        Assert.assertEquals("数据未全部遍历.",expectedOutput.size(),expectedRows);

        //分区读取数据
        List<String> actualOutput = getFragmentsOut(frag_inputs);

        //比较
        Assert.assertFalse("Output did not match expected output",
                compareUnorderedOutput(expectedOutput, actualOutput));

    }

    @Override
    public Class<? extends ReadAccessor> getReadAccessorClass() {
        return JdbcReadAccessor.class;
    }

    @Override
    public Class<? extends ReadResolver> getReadResolverClass() {
        return JdbcReadResolver.class;
    }
}
