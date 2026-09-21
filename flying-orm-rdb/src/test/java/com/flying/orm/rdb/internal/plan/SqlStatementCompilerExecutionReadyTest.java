package com.flying.orm.rdb.internal.plan;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import org.junit.jupiter.api.Test;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlStatementCompilerExecutionReadyTest {

    @Test
    void compilerProducesAnInternalVerifiedPlanThatPublicPreparedCannotForge() {
        SqlStatementPlan verified = SqlStatementCompiler.compile(
                "select * from orders where id = ?",
                1,
                SqlBindMarkerStyle.CANONICAL,
                "PostgreSQL");
        SqlStatementPlan publicPrepared = SqlStatementPlan.prepared(
                "select * from orders where id = ?",
                SqlBindMarkerStyle.CANONICAL,
                1,
                "POSTGRESQL",
                "delete from orders where id = $1");

        assertEquals("VerifiedSqlStatementPlan", verified.getClass().getSimpleName());
        assertNotEquals(verified.getClass(), publicPrepared.getClass());
        assertEquals("select * from orders where id = $1",
                     verified.transportSql("postgresql").orElseThrow());
    }

    @Test
    void batchRequestComposesTheSingleStatementPlanInsteadOfRepeatingSqlFacts() {
        Set<String> components = Arrays.stream(BatchWriteRequest.class.getRecordComponents())
                                       .map(RecordComponent::getName)
                                       .collect(Collectors.toSet());

        assertTrue(components.contains("statement"));
        assertFalse(components.contains("sql"));
        assertFalse(components.contains("parameterCount"));
        assertFalse(components.contains("bindMarkerStyle"));
    }
}
