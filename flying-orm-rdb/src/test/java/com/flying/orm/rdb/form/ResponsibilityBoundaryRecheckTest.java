package com.flying.orm.rdb.form;

import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.jdbc.JdbcBatchWriter;
import com.flying.orm.rdb.jdbc.JdbcSqlExecutor;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import io.r2dbc.spi.ConnectionFactories;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class ResponsibilityBoundaryRecheckTest {

    @Test
    void ordinaryReactiveQueryBindsParametersWithoutAnOrmDeadline() {
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(
                com.flying.orm.rdb.execution.ConnectionAccessTestSupport.reactive(ConnectionFactories.get(
                "r2dbc:h2:mem:///boundary_recheck_" + UUID.randomUUID().toString().replace("-", ""))), RdbDialect.h2());

        var rows = executor.query(new SqlRequest("SELECT ? AS id", List.of(7)),
                SqlExecutionOptions.safeDefaults()).collectList().block();

        assertNotNull(rows);
        assertEquals(1, rows.size());
    }

    @Test
    void derivedSyncClientsConfigureCapacityWithoutAcquiringConnections() {
        SyncFormClient client = clientWithoutConnections();
        SyncFormClient configured = client
                .withDefaultExecutionOptions(SqlExecutionOptions.maxRows(100).withFetchSize(32))
                .withDefaultBatchWriteOptions(BatchWriteOptions.of(10));

        assertNotSame(client, configured);
        assertEquals(BatchWriteOptions.of(10), configured.defaultBatchWriteOptions());
    }

    @Test
    void syncClientDoesNotKeepPerInstanceDurationState() {
        assertFalse(Arrays.stream(SyncFormClient.class.getDeclaredFields())
                .anyMatch(field -> !Modifier.isStatic(field.getModifiers())
                        && field.getType() == Duration.class));
    }

    @Test
    void syncClientConstructorsDoNotPropagateAnUnusedDuration() {
        assertFalse(Arrays.stream(SyncFormClient.class.getDeclaredConstructors())
                .flatMap(constructor -> Arrays.stream(constructor.getParameterTypes()))
                .anyMatch(type -> type == Duration.class));
    }

    private static SyncFormClient clientWithoutConnections() {
        DataSource source = (DataSource) Proxy.newProxyInstance(
                DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("toString")) {
                        return "configuration-only-data-source";
                    }
                    throw new AssertionError("configuration must not access the data source: " + method.getName());
                });
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
        return SyncFormClient.create(
                JdbcSqlExecutor.create(com.flying.orm.rdb.execution.ConnectionAccessTestSupport.jdbc(source), RdbDialect.h2()),
                JdbcBatchWriter.create(com.flying.orm.rdb.execution.ConnectionAccessTestSupport.jdbc(source), RdbDialect.h2()), renderer);
    }
}
