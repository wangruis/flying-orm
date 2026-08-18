package com.flying.orm.rdb.metadata;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证原生 JDBC 元数据 reader 的编排边界。
 *
 * <p>这里用同步执行器替身，不连接真实数据库；真实数据库认证留给最终兼容测试。重点是确认 JDBC
 * reader 使用现有方言 SQL，并且和响应式 reader 得到同样的字段、索引、外键结果。</p>
 */
class JdbcFormMetadataReaderTest {

    @Test
    void readsTableMetadataThroughNativeSyncExecutor() {
        RecordingExecutor executor = new RecordingExecutor(
                List.of(row("COLUMN_NAME", "id", "DATA_TYPE", "bigint", "PRIMARY_KEY", true),
                        row("COLUMN_NAME", "name", "DATA_TYPE", "varchar",
                             "CHARACTER_MAXIMUM_LENGTH", 64)),
                List.of(row("INDEX_NAME", "UK_USERS_NAME", "COLUMN_NAME", "name", "UNIQUE_INDEX", true)),
                List.of(row("FOREIGN_KEY_NAME", "FK_USERS_ORG", "COLUMN_NAME", "org_id",
                             "REFERENCED_TABLE_NAME", "org", "REFERENCED_COLUMN_NAME", "id")));

        JdbcFormMetadataReader reader = JdbcFormMetadataReaders.create(executor, RdbDialect.h2());

        var table = reader.readTable("PUBLIC", "users");

        assertEquals("PUBLIC.users", table.name());
        assertEquals(2, table.columns().size());
        assertTrue(table.column("id").primaryKey());
        assertEquals(64, table.column("name").length());
        assertTrue(table.index("UK_USERS_NAME").unique());
        assertEquals(List.of("name"), table.index("UK_USERS_NAME").columns());
        assertEquals("org", table.foreignKey("FK_USERS_ORG").referenceTable());
        assertEquals(3, executor.requests.size());
    }

    @Test
    void readsFormWithoutReactiveBridge() {
        RecordingExecutor executor = new RecordingExecutor(
                List.of(row("COLUMN_NAME", "id", "DATA_TYPE", "integer", "PRIMARY_KEY", 1)));

        JdbcFormMetadataReader reader = JdbcFormMetadataReaders.create(executor, RdbDialect.h2());

        var form = reader.readForm("users", "USERS");

        assertEquals("users", form.id());
        assertEquals("USERS", form.table());
        assertEquals("INTEGER", form.field("id").dataType());
        assertTrue(form.field("id").primaryKey());
        assertEquals(1, executor.requests.size());
    }

    /** 不支持的自定义方言只能返回稳定分类，不能把调用方提供的无界名称复制进公开错误。 */
    @Test
    void unsupportedDialectDoesNotEchoCallerName() {
        String secretName = "secret-dialect-" + "x".repeat(8_192);
        RdbDialect base = RdbDialect.h2();
        RdbDialect custom = RdbDialect.of(
                secretName, base.schema(), base.pagination(), base.upsert(), base.json());

        UnsupportedOperationException error = assertThrows(
                UnsupportedOperationException.class,
                () -> JdbcFormMetadataReaders.create(new RepeatingExecutor(), custom));

        assertEquals("metadata reader is not implemented for the requested dialect", error.getMessage());
    }

    /** 同步公开门面必须直接落到 JDBC reader，不能为了保持旧 API 又绕回应式等待。 */
    @Test
    void syncFacadeUsesNativeJdbcReader() {
        RecordingExecutor executor = new RecordingExecutor(
                List.of(row("COLUMN_NAME", "id", "DATA_TYPE", "integer", "PRIMARY_KEY", true)));

        SyncFormMetadataReader reader = SyncFormMetadataReader.create(executor, RdbDialect.h2());

        var form = reader.readForm("users", "USERS");

        assertEquals("users", form.id());
        assertEquals("USERS", form.table());
        assertEquals(1, executor.requests.size());
    }

    @Test
    void cachesJdbcMetadataAndInvalidatesDependentPlansTogether() {
        RepeatingExecutor executor = new RepeatingExecutor();
        AtomicInteger dependentInvalidations = new AtomicInteger();
        MetadataCacheInvalidator dependent = new MetadataCacheInvalidator() {
            @Override
            public void invalidate(String table) {
                dependentInvalidations.incrementAndGet();
            }

            @Override
            public void invalidateAll() {
                dependentInvalidations.incrementAndGet();
            }
        };
        JdbcFormMetadataReader reader = JdbcFormMetadataReaders.cached(
                executor, RdbDialect.h2(), CacheRegionPolicy.metadataDefaults(), dependent);

        reader.readForm("users", "USERS");
        reader.readForm("users", "USERS");
        assertEquals(1, executor.queries.get());

        reader.invalidate("USERS");
        reader.readForm("users", "USERS");

        assertEquals(2, executor.queries.get());
        assertEquals(1, dependentInvalidations.get());
    }

    /** 同步动态数据源切换物理路由后，同名表必须重新读取元数据；切回原路由仍可命中原分区。 */
    @Test
    void isolatesJdbcMetadataByCurrentRoutingIdentity() {
        RepeatingExecutor executor = new RepeatingExecutor();
        JdbcFormMetadataReader reader = JdbcFormMetadataReaders.cached(
                executor, RdbDialect.h2(), CacheRegionPolicy.metadataDefaults(), MetadataCacheInvalidator.none());

        executor.partition.set("primary");
        var primary = reader.readForm("users", "USERS");
        assertEquals(primary, reader.readForm("users", "USERS"));
        executor.partition.set("archive");
        var archive = reader.readForm("users", "USERS");
        executor.partition.set("primary");

        assertTrue(primary != archive);
        assertEquals(primary, reader.readForm("users", "USERS"));
        assertEquals(2, executor.queries.get());
    }

    /** 外部事务可能看到未提交 DDL，事务内元数据不能读写进程级共享缓存。 */
    @Test
    void bypassesSharedMetadataCacheInsideExternalTransaction() {
        RepeatingExecutor executor = new RepeatingExecutor();
        JdbcFormMetadataReader reader = JdbcFormMetadataReaders.cached(
                executor, RdbDialect.h2(), CacheRegionPolicy.metadataDefaults(), MetadataCacheInvalidator.none());
        executor.transaction.set(JdbcTransactionContext.external(transactionConnection(), "primary"));

        var first = reader.readForm("users", "USERS");
        var second = reader.readForm("users", "USERS");
        executor.transaction.set(null);
        var outside = reader.readForm("users", "USERS");

        assertNotSame(first, second);
        assertNotSame(second, outside);
        assertSame(outside, reader.readForm("users", "USERS"));
        assertEquals(3, executor.queries.get());
    }

    /** 表级元数据包含字段、索引和外键，整组结果在外部事务中都必须旁路共享缓存。 */
    @Test
    void bypassesSharedTableMetadataCacheInsideExternalTransaction() {
        RecordingExecutor executor = new RecordingExecutor(
                List.of(row("COLUMN_NAME", "id", "DATA_TYPE", "bigint", "PRIMARY_KEY", true)),
                List.of(),
                List.of());
        JdbcFormMetadataReader reader = JdbcFormMetadataReaders.cached(
                executor, RdbDialect.h2(), CacheRegionPolicy.metadataDefaults(), MetadataCacheInvalidator.none());
        executor.transaction.set(JdbcTransactionContext.external(transactionConnection(), "primary"));

        var first = reader.readTable("PUBLIC", "USERS");
        var second = reader.readTable("PUBLIC", "USERS");
        executor.transaction.set(null);
        var outside = reader.readTable("PUBLIC", "USERS");

        assertNotSame(first, second);
        assertNotSame(second, outside);
        assertSame(outside, reader.readTable("PUBLIC", "USERS"));
        assertEquals(9, executor.requests.size());
    }

    private static Map<String, Object> row(Object... values) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int index = 0; index < values.length; index += 2) {
            row.put((String) values[index], values[index + 1]);
        }
        return row;
    }

    private static final class RecordingExecutor implements SyncSqlExecutor {

        private final List<List<DynamicRow>> results;
        private final List<SqlRequest> requests = new ArrayList<>();
        private final AtomicReference<JdbcTransactionContext> transaction = new AtomicReference<>();

        private RecordingExecutor(List<Map<String, Object>> columns) {
            this(columns, List.of(), List.of());
        }

        private RecordingExecutor(List<Map<String, Object>> columns,
                                  List<Map<String, Object>> indexes,
                                  List<Map<String, Object>> foreignKeys) {
            this.results = List.of(toRows(columns), toRows(indexes), toRows(foreignKeys));
        }

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            requests.add(request);
            int index = requests.size() - 1;
            return results.get(index % results.size());
        }

        @Override
        public Optional<JdbcTransactionContext> currentTransaction() {
            return Optional.ofNullable(transaction.get());
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new UnsupportedOperationException("metadata test executor does not write");
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            throw new UnsupportedOperationException("metadata test executor does not write");
        }

        private static List<DynamicRow> toRows(List<Map<String, Object>> rows) {
            return rows.stream().map(DynamicRow::copyOf).toList();
        }
    }

    private static final class RepeatingExecutor implements SyncSqlExecutor {
        private final AtomicInteger queries = new AtomicInteger();
        private final AtomicReference<String> partition = new AtomicReference<>();
        private final AtomicReference<JdbcTransactionContext> transaction = new AtomicReference<>();

        @Override
        public String metadataCachePartition() {
            return partition.get();
        }

        @Override
        public Optional<JdbcTransactionContext> currentTransaction() {
            return Optional.ofNullable(transaction.get());
        }

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            queries.incrementAndGet();
            return List.of(DynamicRow.copyOf(
                    row("COLUMN_NAME", "id", "DATA_TYPE", "integer", "PRIMARY_KEY", true)));
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new UnsupportedOperationException("metadata test executor does not write");
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            throw new UnsupportedOperationException("metadata test executor does not write");
        }
    }

    private static Connection transactionConnection() {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                                                    new Class<?>[]{Connection.class},
                                                    (proxy, method, arguments) -> null);
    }
}
