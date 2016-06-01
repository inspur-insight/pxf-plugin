package com.insight.pxf.plugins.jdbc;

import com.insight.pxf.PxfUnit;
import org.apache.hawq.pxf.api.ReadAccessor;
import org.apache.hawq.pxf.api.ReadResolver;
import org.junit.Assert;

import java.util.List;

/**
 * Created by jiadx on 2016/5/26.
 */
public abstract class JdbcUnit extends PxfUnit {

    @Override
    public Class<? extends ReadAccessor> getReadAccessorClass() {
        return JdbcReadAccessor.class;
    }

    @Override
    public Class<? extends ReadResolver> getReadResolverClass() {
        return JdbcReadResolver.class;
    }
}
