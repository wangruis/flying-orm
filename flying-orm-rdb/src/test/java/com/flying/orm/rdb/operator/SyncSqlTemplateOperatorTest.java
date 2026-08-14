package com.flying.orm.rdb.operator;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.template.SqlTemplate;
import com.flying.orm.rdb.template.SqlTemplateEngine;
import com.flying.orm.rdb.template.SqlTemplateRegistry;
import com.flying.orm.rdb.template.SyncSqlTemplateParameterProvider;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 验证同步模板直接获取可信参数，且不让调用方伪造租户等服务端值。 */
class SyncSqlTemplateOperatorTest {

    @Test
    void rendersTrustedParametersAndControlledIdentifiersBeforeJdbcExecution() {
        RecordingExecutor executor = new RecordingExecutor(List.of(DynamicRow.copyOf(Map.of("id", "u-1"))));
        SyncSqlTemplateOperator operator = template(executor, (templateId, names) -> Map.of("tenantId", "t-1"));

        List<DynamicRow> rows = operator.identifier("table", "users")
                                        .bind("id", "u-1")
                                        .query();

        assertEquals("select id from `users` where tenant_id = ? and id = ?", executor.request.sql());
        assertEquals(List.of("t-1", "u-1"), executor.request.parameters());
        assertEquals("u-1", rows.getFirst().get("id"));
    }

    @Test
    void rejectsForgedOrIncompleteServerParametersBeforeExecutionAndKeepsOneSemantics() {
        RecordingExecutor executor = new RecordingExecutor(List.of());
        SyncSqlTemplateOperator forged = template(executor, (templateId, names) -> Map.of("tenantId", "t-1"));
        IllegalArgumentException forgedError = assertThrows(
                IllegalArgumentException.class, () -> forged.bind("tenantId", "forged"));
        assertFalse(forgedError.getMessage().contains("tenantId"));

        SyncSqlTemplateOperator incomplete = template(executor, (templateId, names) -> Map.of());
        IllegalArgumentException incompleteError = assertThrows(
                IllegalArgumentException.class, () -> incomplete.bind("id", "u-1").query());
        assertFalse(incompleteError.getMessage().contains("tenantId"));
        assertNull(executor.request);

        assertNull(template(executor, (templateId, names) -> Map.of("tenantId", "t-1"))
                .identifier("table", "users")
                .bind("id", "u-1")
                .one());
    }

    /** 同步模板与同步原生 SQL 一样，PostgreSQL JDBC 只能接收问号参数标记。 */
    @Test
    void postgresqlTemplateUsesJdbcQuestionMarkers() {
        SqlTemplateRegistry registry = SqlTemplateRegistry.builder()
                .register(SqlTemplate.query("pg-user", "select id from users where tenant_id = :tenantId", Set.of()))
                .build();
        ValueCodecRegistry codecs = ValueCodecRegistry.standard();

        SqlRequest request = SqlTemplateEngine.create(registry, RdbDialect.postgresql(), codecs)
                .forJdbc()
                .render("pg-user", Map.of("tenantId", "t-1"), Map.of());

        assertEquals("select id from users where tenant_id = ?", request.sql());
        assertEquals(List.of("t-1"), request.parameters());
    }

    private static SyncSqlTemplateOperator template(RecordingExecutor executor,
                                                     SyncSqlTemplateParameterProvider parameters) {
        SqlTemplateRegistry registry = SqlTemplateRegistry.builder()
                .register(SqlTemplate.query("user-by-id",
                                            "select id from ${table} where tenant_id = :tenantId and id = :id",
                                            Set.of("table")),
                          Set.of("tenantId"))
                .build();
        ValueCodecRegistry codecs = ValueCodecRegistry.standard();
        return new SyncSqlTemplateOperator(executor,
                                           codecs,
                                           EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults()),
                                           SqlTemplateEngine.create(registry, RdbDialect.mysql(), codecs).forJdbc(),
                                           registry.template("user-by-id"),
                                           registry.serverParameters("user-by-id"),
                                           parameters);
    }

    private static final class RecordingExecutor implements SyncSqlExecutor {

        private final List<DynamicRow> rows;
        private SqlRequest request;

        private RecordingExecutor(List<DynamicRow> rows) {
            this.rows = rows;
        }

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            this.request = request;
            return rows;
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new AssertionError("registered query templates must not execute as writes");
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            throw new AssertionError("registered query templates must not execute as writes");
        }
    }
}
