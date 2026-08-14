package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 不连真实生产库，只验证 MySQL/PostgreSQL 元数据读取器的 SQL 参数和行转换规则。
 *
 * @author wangr
 * @date 2026-07-28
 * @version v1.0
 */
class ProductionReactiveFormMetadataReaderTest {

    /**
     * 裸表名必须按数据库自身的可见 Schema 解析，不能把其它可见 Schema 中的同名表元数据合并进来。
     */
    @Test
    void scopesUnqualifiedTableMetadataToTheDatabaseVisibleSchema() {
        assertUnqualifiedMetadata(H2ReactiveFormMetadataReader::create,
                                  "c.TABLE_SCHEMA = current_schema()",
                                  "i.TABLE_SCHEMA = current_schema()",
                                  "tc.TABLE_SCHEMA = current_schema()",
                                  List.of("users", "users", "users"));
        assertUnqualifiedMetadata(MySqlReactiveFormMetadataReader::create,
                                  "c.TABLE_SCHEMA = DATABASE()",
                                  "s.TABLE_SCHEMA = DATABASE()",
                                  "kcu.TABLE_SCHEMA = DATABASE()",
                                  List.of("users"));
        assertUnqualifiedMetadata(PostgreSqlReactiveFormMetadataReader::create,
                                  "pg_catalog.pg_table_is_visible(visible_table.oid)",
                                  "pg_catalog.pg_table_is_visible(t.oid)",
                                  "pg_catalog.pg_table_is_visible(t.oid)",
                                  List.of("users"));
        assertUnqualifiedMetadata(OracleReactiveFormMetadataReader::create,
                                  "c.OWNER = sys_context('USERENV', 'CURRENT_SCHEMA')",
                                  "i.TABLE_OWNER = sys_context('USERENV', 'CURRENT_SCHEMA')",
                                  "ac.OWNER = sys_context('USERENV', 'CURRENT_SCHEMA')",
                                  List.of("users", "users", "users"));
        assertUnqualifiedMetadata(SqlServerReactiveFormMetadataReader::create,
                                  "t.object_id = object_id(?)",
                                  "t.object_id = object_id(?)",
                                  "pt.object_id = object_id(?)",
                                  List.of("users", "users"));
    }

    @Test
    void doesNotEchoRuntimeTableNameWhenMetadataIsMissing() {
        String table = "users--must-not-leak";
        RecordingExecutor executor = new RecordingExecutor(List.<Map<String, Object>>of());
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> MySqlReactiveFormMetadataReader.create(executor)
                                                   .readForm("users", table)
                                                   .block());

        assertEquals("table metadata not found", error.getMessage());
        assertFalse(error.getMessage().contains(table));
    }

    /**
     * MySQL reader 要能把 information_schema.columns 的常见类型转成动态表单字段。
     */
    @Test
    void readsMysqlInformationSchemaRows() {
        RecordingExecutor executor = new RecordingExecutor(List.of(
                row("COLUMN_NAME", "id", "DATA_TYPE", "bigint", "PRIMARY_KEY", 1),
                row("COLUMN_NAME", "name", "DATA_TYPE", "varchar", "CHARACTER_MAXIMUM_LENGTH", 128,
                    "REMARKS", "Name"),
                row("COLUMN_NAME", "amount", "DATA_TYPE", "decimal", "NUMERIC_PRECISION", 18,
                    "NUMERIC_SCALE", 2)
        ));

        StepVerifier.create(MySqlReactiveFormMetadataReader.create(executor).readForm("users", "app", "users"))
                    .assertNext(form -> {
                        assertEquals("app.users", form.table());
                        DynamicField id = form.field("id");
                        assertTrue(id.primaryKey());
                        assertEquals("BIGINT", id.dataType());

                        DynamicField name = form.field("name");
                        assertEquals("VARCHAR", name.dataType());
                        assertEquals(128, name.length());
                        assertEquals("Name", name.comment());

                        DynamicField amount = form.field("amount");
                        assertEquals("DECIMAL", amount.dataType());
                        assertEquals(18, amount.precision());
                        assertEquals(2, amount.scale());
                    })
                    .verifyComplete();

        assertEquals(List.of("users", "app"), executor.lastRequest().parameters());
        assertTrue(executor.lastRequest().sql().contains("join information_schema.TABLE_CONSTRAINTS pk_tc"));
    }

    /** 二进制列的已知容量必须保留下来，Schema 校验才能拒绝过短的保护哈希列。 */
    @Test
    void retainsKnownBinaryLengthForProtectedSchemaValidation() {
        RecordingExecutor executor = new RecordingExecutor(List.of(
                row("COLUMN_NAME", "token_hash", "DATA_TYPE", "binary",
                    "CHARACTER_MAXIMUM_LENGTH", 16)
        ));

        StepVerifier.create(MySqlReactiveFormMetadataReader.create(executor)
                                                           .readForm("tokens", "app", "tokens"))
                    .assertNext(form -> {
                        assertEquals("BLOB", form.field("token_hash").dataType());
                        assertEquals(16, form.field("token_hash").length());
                    })
                    .verifyComplete();
    }

    /**
     * PostgreSQL reader 使用同一套 DynamicForm 转换，但类型名来自 PostgreSQL 的 information_schema。
     */
    @Test
    void readsPostgresqlInformationSchemaRows() {
        RecordingExecutor executor = new RecordingExecutor(List.of(
                row("COLUMN_NAME", "id", "DATA_TYPE", "bigint", "PRIMARY_KEY", true),
                row("COLUMN_NAME", "enabled", "DATA_TYPE", "boolean"),
                row("COLUMN_NAME", "name", "DATA_TYPE", "character varying", "CHARACTER_MAXIMUM_LENGTH", 64),
                row("COLUMN_NAME", "meeting_time", "DATA_TYPE", "time with time zone"),
                row("COLUMN_NAME", "small_numbers", "DATA_TYPE", "int2[]")
        ));

        StepVerifier.create(PostgreSqlReactiveFormMetadataReader.create(executor).readForm("users", "public", "users"))
                    .assertNext(form -> {
                        assertEquals("public.users", form.table());
                        assertEquals("BIGINT", form.field("id").dataType());
                        assertEquals("BOOLEAN", form.field("enabled").dataType());
                        assertEquals("VARCHAR", form.field("name").dataType());
                        assertEquals(64, form.field("name").length());
                        assertEquals("OFFSET_TIME", form.field("meeting_time").dataType());
                        assertEquals("SMALLINT[]", form.field("small_numbers").dataType());
                    })
                    .verifyComplete();

        assertEquals(List.of("users", "public"), executor.lastRequest().parameters());
        assertEquals(SqlBindMarkerStyle.CANONICAL, executor.lastRequest().bindMarkerStyle());
    }

    /** PostgreSQL 与 SQL Server 必须先筛出主键约束，避免一列参与多个键约束时产生重复元数据行。 */
    @Test
    void filtersPrimaryKeyConstraintsBeforeJoiningColumnMetadata() {
        RecordingExecutor postgresql = new RecordingExecutor(List.of(
                row("COLUMN_NAME", "id", "DATA_TYPE", "bigint", "PRIMARY_KEY", true)));
        RecordingExecutor sqlServer = new RecordingExecutor(List.of(
                row("COLUMN_NAME", "id", "DATA_TYPE", "bigint", "PRIMARY_KEY", true)));

        PostgreSqlReactiveFormMetadataReader.create(postgresql).readForm("users", "app", "users").block();
        SqlServerReactiveFormMetadataReader.create(sqlServer).readForm("users", "app", "users").block();

        String postgresqlSql = normalizedSql(postgresql.request(0).sql());
        String sqlServerSql = normalizedSql(sqlServer.request(0).sql());
        assertTrue(postgresqlSql.contains("left join ( select pk_kcu.table_schema"));
        assertTrue(postgresqlSql.contains("join information_schema.table_constraints pk_tc"));
        assertFalse(postgresqlSql.contains("left join information_schema.key_column_usage kcu"));
        assertTrue(sqlServerSql.contains("left join ( select pk_kcu.table_schema"));
        assertTrue(sqlServerSql.contains("join information_schema.table_constraints pk_tc"));
        assertFalse(sqlServerSql.contains("left join information_schema.key_column_usage kcu"));
    }

    /**
     * 方言工厂要直接给 H2/MySQL/PostgreSQL 返回真实 reader，其它库暂时明确报不支持。
     */
    @Test
    void readsOracleDictionaryRows() {
        RecordingExecutor executor = new RecordingExecutor(List.of(
                row("COLUMN_NAME", "ID", "DATA_TYPE", "NUMBER", "NUMERIC_PRECISION", 19,
                    "NUMERIC_SCALE", 0, "PRIMARY_KEY", "true"),
                row("COLUMN_NAME", "NAME", "DATA_TYPE", "VARCHAR2", "CHARACTER_MAXIMUM_LENGTH", 100,
                    "REMARKS", "Name"),
                row("COLUMN_NAME", "BIO", "DATA_TYPE", "CLOB")
        ));

        StepVerifier.create(OracleReactiveFormMetadataReader.create(executor).readForm("users", "APP", "USERS"))
                    .assertNext(form -> {
                        assertEquals("APP.USERS", form.table());
                        assertTrue(form.field("ID").primaryKey());
                        assertEquals("DECIMAL", form.field("ID").dataType());
                        assertEquals(19, form.field("ID").precision());
                        assertEquals(0, form.field("ID").scale());
                        assertEquals("VARCHAR", form.field("NAME").dataType());
                        assertEquals(100, form.field("NAME").length());
                        assertEquals("Name", form.field("NAME").comment());
                        assertEquals("TEXT", form.field("BIO").dataType());
                    })
                    .verifyComplete();

        assertEquals(List.of("USERS", "USERS", "USERS", "APP", "APP", "APP"),
                     executor.lastRequest().parameters());
    }

    /** Oracle 优先精确 quoted 名称；精确对象不存在时仍支持未加引号名称折叠为大写。 */
    @Test
    void prefersExactOracleMetadataNamesBeforeUnquotedUppercaseFallback() {
        RecordingExecutor executor = RecordingExecutor.responses(
                List.of(row("COLUMN_NAME", "id", "DATA_TYPE", "number")),
                List.of(),
                List.of());

        StepVerifier.create(OracleReactiveFormMetadataReader.create(executor)
                                                            .readTable("App", "CaseTable"))
                    .assertNext(table -> assertEquals("App.CaseTable", table.name()))
                    .verifyComplete();

        assertTrue(executor.request(0).sql().contains("c.TABLE_NAME = case"));
        assertTrue(executor.request(0).sql().contains("c.OWNER = case"));
        assertTrue(executor.request(1).sql().contains("i.TABLE_NAME = case"));
        assertTrue(executor.request(1).sql().contains("i.TABLE_OWNER = case"));
        assertTrue(executor.request(2).sql().contains("ac.TABLE_NAME = case"));
        assertTrue(executor.request(2).sql().contains("ac.OWNER = case"));
        for (int index = 0; index < 3; index++) {
            assertTrue(executor.request(index).sql().contains("then ? else upper(?) end"));
            assertEquals(List.of("CaseTable", "CaseTable", "CaseTable", "App", "App", "App"),
                         executor.request(index).parameters());
        }
    }

    @Test
    void readsSqlServerInformationSchemaRows() {
        RecordingExecutor executor = new RecordingExecutor(List.of(
                row("COLUMN_NAME", "id", "DATA_TYPE", "bigint", "PRIMARY_KEY", true),
                row("COLUMN_NAME", "name", "DATA_TYPE", "nvarchar", "CHARACTER_MAXIMUM_LENGTH", 128,
                    "REMARKS", "Name"),
                row("COLUMN_NAME", "enabled", "DATA_TYPE", "bit")
        ));

        StepVerifier.create(SqlServerReactiveFormMetadataReader.create(executor).readForm("users", "dbo", "Users"))
                    .assertNext(form -> {
                        assertEquals("dbo.Users", form.table());
                        assertEquals("BIGINT", form.field("id").dataType());
                        assertTrue(form.field("id").primaryKey());
                        assertEquals("VARCHAR", form.field("name").dataType());
                        assertEquals(128, form.field("name").length());
                        assertEquals("Name", form.field("name").comment());
                        assertEquals("BOOLEAN", form.field("enabled").dataType());
                    })
                    .verifyComplete();

        assertEquals(List.of("Users", "dbo"), executor.lastRequest().parameters());
    }

    @Test
    void readsProductionIndexesThroughSharedTableMetadataContract() {
        List<Object> regularParameters = List.of("users", "app");
        assertProductionIndexes(MySqlReactiveFormMetadataReader::create, "information_schema.STATISTICS",
                                regularParameters);
        assertProductionIndexes(PostgreSqlReactiveFormMetadataReader::create, "pg_catalog.pg_index",
                                regularParameters);
        assertProductionIndexes(OracleReactiveFormMetadataReader::create, "ALL_INDEXES",
                                List.of("users", "users", "users", "app", "app", "app"));
        assertProductionIndexes(SqlServerReactiveFormMetadataReader::create, "sys.indexes", regularParameters);
    }

    @Test
    void readsProductionForeignKeysThroughSharedTableMetadataContract() {
        List<Object> regularParameters = List.of("users", "app");
        assertProductionForeignKeys(MySqlReactiveFormMetadataReader::create,
                                    "information_schema.KEY_COLUMN_USAGE", regularParameters);
        assertProductionForeignKeys(PostgreSqlReactiveFormMetadataReader::create, "pg_catalog.pg_constraint",
                                    regularParameters);
        assertProductionForeignKeys(OracleReactiveFormMetadataReader::create, "ALL_CONSTRAINTS",
                                    List.of("users", "users", "users", "app", "app", "app"));
        assertProductionForeignKeys(SqlServerReactiveFormMetadataReader::create, "sys.foreign_keys",
                                    regularParameters);
    }

    @Test
    void createsReaderByDialect() {
        RecordingExecutor executor = new RecordingExecutor(List.<Map<String, Object>>of());

        assertInstanceOf(H2ReactiveFormMetadataReader.class,
                         ReactiveFormMetadataReaders.create(executor, RdbDialect.h2()));
        assertInstanceOf(MySqlReactiveFormMetadataReader.class,
                         ReactiveFormMetadataReaders.create(executor, RdbDialect.mysql()));
        assertInstanceOf(PostgreSqlReactiveFormMetadataReader.class,
                         ReactiveFormMetadataReaders.create(executor, RdbDialect.postgresql()));
        assertInstanceOf(OracleReactiveFormMetadataReader.class,
                         ReactiveFormMetadataReaders.create(executor, RdbDialect.oracle()));
        assertInstanceOf(SqlServerReactiveFormMetadataReader.class,
                         ReactiveFormMetadataReaders.create(executor, RdbDialect.sqlServer()));
    }

    private static Map<String, Object> row(Object... pairs) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            values.put((String) pairs[i], pairs[i + 1]);
        }
        return values;
    }

    private static String normalizedSql(String sql) {
        return sql.replaceAll("\\s+", " ").trim().toLowerCase(Locale.ROOT);
    }

    private static void assertProductionIndexes(ReaderFactory readerFactory,
                                                String sqlMarker,
                                                List<Object> expectedParameters) {
        RecordingExecutor executor = RecordingExecutor.responses(
                List.of(
                        row("COLUMN_NAME", "id", "DATA_TYPE", "bigint", "PRIMARY_KEY", true),
                        row("COLUMN_NAME", "name", "DATA_TYPE", "varchar", "CHARACTER_MAXIMUM_LENGTH", 64),
                        row("COLUMN_NAME", "org_id", "DATA_TYPE", "bigint")
                ),
                List.of(
                row("INDEX_NAME", "uk_users_name", "COLUMN_NAME", "name", "UNIQUE_INDEX", 1),
                row("INDEX_NAME", "idx_users_org", "COLUMN_NAME", "org_id", "UNIQUE_INDEX", 0)
                ),
                List.of());

        StepVerifier.create(readerFactory.create(executor).readTable("app", "users"))
                    .assertNext(ProductionReactiveFormMetadataReaderTest::assertTableIndexes)
                    .verifyComplete();

        assertEquals(expectedParameters, executor.request(1).parameters());
        assertTrue(executor.request(1).sql().contains(sqlMarker));
    }

    private static void assertProductionForeignKeys(ReaderFactory readerFactory,
                                                    String sqlMarker,
                                                    List<Object> expectedParameters) {
        RecordingExecutor executor = RecordingExecutor.responses(
                List.of(
                        row("COLUMN_NAME", "id", "DATA_TYPE", "bigint", "PRIMARY_KEY", true),
                        row("COLUMN_NAME", "org_id", "DATA_TYPE", "bigint"),
                        row("COLUMN_NAME", "parent_id", "DATA_TYPE", "bigint")
                ),
                List.of(),
                List.of(
                        row("FOREIGN_KEY_NAME", "fk_users_org", "COLUMN_NAME", "org_id",
                            "REFERENCED_TABLE_NAME", "org", "REFERENCED_COLUMN_NAME", "id"),
                        row("FOREIGN_KEY_NAME", "fk_users_parent", "COLUMN_NAME", "parent_id",
                            "REFERENCED_TABLE_NAME", "users", "REFERENCED_COLUMN_NAME", "id")
                ));

        StepVerifier.create(readerFactory.create(executor).readTable("app", "users"))
                    .assertNext(ProductionReactiveFormMetadataReaderTest::assertTableForeignKeys)
                    .verifyComplete();

        assertEquals(expectedParameters, executor.request(2).parameters());
        assertTrue(executor.request(2).sql().contains(sqlMarker));
    }

    private static void assertUnqualifiedMetadata(ReaderFactory readerFactory,
                                                  String columnSchemaPredicate,
                                                  String indexSchemaPredicate,
                                                  String foreignKeySchemaPredicate,
                                                  List<Object> expectedParameters) {
        RecordingExecutor executor = RecordingExecutor.responses(
                List.of(row("COLUMN_NAME", "id", "DATA_TYPE", "varchar")),
                List.of(),
                List.of());

        StepVerifier.create(readerFactory.create(executor).readTable("users"))
                    .assertNext(table -> assertEquals("users", table.name()))
                    .verifyComplete();

        assertTrue(executor.request(0).sql().contains(columnSchemaPredicate));
        assertTrue(executor.request(1).sql().contains(indexSchemaPredicate));
        assertTrue(executor.request(2).sql().contains(foreignKeySchemaPredicate));
        assertEquals(expectedParameters, executor.request(0).parameters());
        assertEquals(expectedParameters, executor.request(1).parameters());
        assertEquals(expectedParameters, executor.request(2).parameters());
    }

    private static void assertTableIndexes(TableMetadata table) {
        assertEquals("app.users", table.name());
        assertEquals(List.of("name"), table.index("uk_users_name").columns());
        assertTrue(table.index("uk_users_name").unique());
        assertEquals(List.of("org_id"), table.index("idx_users_org").columns());
    }

    private static void assertTableForeignKeys(TableMetadata table) {
        assertEquals("app.users", table.name());
        assertEquals(List.of("org_id"), table.foreignKey("fk_users_org").columns());
        assertEquals("org", table.foreignKey("fk_users_org").referenceTable());
        assertEquals(List.of("id"), table.foreignKey("fk_users_org").referenceColumns());
        assertEquals(List.of("parent_id"), table.foreignKey("fk_users_parent").columns());
        assertEquals("users", table.foreignKey("fk_users_parent").referenceTable());
    }

    @FunctionalInterface
    private interface ReaderFactory {

        ReactiveFormMetadataReader create(ReactiveSqlExecutor executor);
    }

    private static final class RecordingExecutor implements ReactiveSqlExecutor {

        private final List<List<Map<String, Object>>> responses;

        private final List<SqlRequest> requests = new ArrayList<>();

        private RecordingExecutor(List<Map<String, Object>> rows) {
            this.responses = List.of(rows);
        }

        private RecordingExecutor(Collection<List<Map<String, Object>>> responses) {
            this.responses = List.copyOf(responses);
        }

        @SafeVarargs
        private static RecordingExecutor responses(List<Map<String, Object>>... responses) {
            return new RecordingExecutor(List.of(responses));
        }

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            requests.add(request);
            int responseIndex = Math.min(requests.size() - 1, responses.size() - 1);
            return Flux.fromIterable(responses.get(responseIndex)).map(DynamicRow::copyOf);
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            return Mono.error(new UnsupportedOperationException("metadata reader test must not update rows"));
        }

        private SqlRequest lastRequest() {
            return requests.getLast();
        }

        private SqlRequest request(int index) {
            return requests.get(index);
        }
    }
}
