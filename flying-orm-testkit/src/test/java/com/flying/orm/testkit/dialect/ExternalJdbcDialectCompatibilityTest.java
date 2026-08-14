package com.flying.orm.testkit.dialect;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 原生 JDBC 的外部数据库兼容认证。
 *
 * <p>默认不会寻找、启动或创建任何数据库。只有显式传入某一库的 JDBC URL 后，对应场景才会运行，例如
 * {@code -Dflying.orm.compat.mysql.jdbc.url=jdbc:mysql://localhost:3306/flying_orm}。用户名和密码可分别用
 * 同前缀的 {@code .user}/{@code .password} 指定；也可以直接写在 JDBC URL 中。</p>
 *
 * <p>四种数据库共用同一套高频业务契约，刻意只把清表 SQL 这一个不可避免的方言差异留在参数中。这里验证
 * V2 的 JDBC 公共门面本身，而不是绕过它直接调用 JDBC executor。</p>
 */
class ExternalJdbcDialectCompatibilityTest {

    @Test
    void verifiesMysqlWhenJdbcUrlIsConfigured() {
        runIfConfigured("mysql", "FLYING_ORM_JDBC_CERT_MYSQL", "drop table if exists `FLYING_ORM_JDBC_CERT_MYSQL`");
    }

    @Test
    void verifiesPostgresqlWhenJdbcUrlIsConfigured() {
        runIfConfigured("postgresql", "FLYING_ORM_JDBC_CERT_PG", "drop table if exists \"FLYING_ORM_JDBC_CERT_PG\"");
    }

    @Test
    void verifiesOracleWhenJdbcUrlIsConfigured() {
        runIfConfigured("oracle", "FLYING_ORM_JDBC_CERT_ORACLE", "drop table \"FLYING_ORM_JDBC_CERT_ORACLE\"");
    }

    @Test
    void verifiesSqlServerWhenJdbcUrlIsConfigured() {
        runIfConfigured("sqlserver", "FLYING_ORM_JDBC_CERT_SQLSERVER",
                        "drop table if exists \"FLYING_ORM_JDBC_CERT_SQLSERVER\"");
    }

    private static void runIfConfigured(String expectedDialect, String tableName, String cleanupSql) {
        String prefix = "flying.orm.compat." + expectedDialect + ".jdbc";
        String url = System.getProperty(prefix + ".url");
        Assumptions.assumeTrue(url != null && !url.isBlank(), prefix + ".url is not configured");

        TrackingDriverManagerDataSource source = new TrackingDriverManagerDataSource(
                url.trim(), System.getProperty(prefix + ".user"), System.getProperty(prefix + ".password"));
        FlyingOrmClients clients = null;
        try {
            clients = FlyingOrmClients.builder(source).build();
            assertTrue(clients.jdbcAvailable());
            assertFalse(clients.reactiveAvailable());

            DynamicForm form = form(tableName);
            executeIgnoringFailure(clients, cleanupSql);
            clients.syncSchema().createTable(form);

            assertEquals(1L, clients.syncForms().insert(WriteSpec.insert(form, row("u-1", "first", 1))));
            List<DynamicRow> selected = clients.syncForms().select(QuerySpec.of(form, byId("u-1")));
            assertEquals(1, selected.size());
            assertEquals("first", selected.getFirst().get("NAME"));

            assertEquals(1L, clients.syncForms().update(
                    WriteSpec.update(form, Map.of("NAME", "second", "AGE", 2), byId("u-1"))));
            RdbDialect dialect = dialect(expectedDialect);
            String quotedTable = dialect.schema().identifier(tableName);
            String quotedName = dialect.schema().identifier("NAME");
            String quotedId = dialect.schema().identifier("ID");
            DynamicRow nativeRow = clients.syncOperator()
                                          .unsafeNativeSql("select " + quotedName + " from " + quotedTable
                                                  + " where " + quotedId + " = :id")
                                          .bind("id", "u-1")
                                          .one();
            // 列标签大小写由驱动决定；按下标读取只验证原生 SQL 值，不把标签习惯混进跨库契约。
            assertEquals("second", nativeRow.value(0));

            assertEquals(1L, clients.syncForms().delete(WriteSpec.delete(form, byId("u-1"))));
            assertTrue(clients.syncForms().select(QuerySpec.of(form, byId("u-1"))).isEmpty());

            BatchWriteResult committed = clients.syncForms().writeBatch(BatchSpec.insert(
                    form, Flux.just(row("b-1", "batch-one", 3), row("b-2", "batch-two", 4)))
                    .withOptions(BatchWriteOptions.atomic(2)));
            assertEquals(BatchWriteResult.Status.COMMITTED, committed.status());
            assertEquals(2, committed.inputCount());

            FlyingOrmClients jdbcClients = clients;
            BatchWriteException duplicateKey = assertThrows(BatchWriteException.class, () -> jdbcClients.syncForms()
                    .writeBatch(BatchSpec.insert(form, Flux.just(row("rollback", "first", 5),
                                                                  row("rollback", "second", 6)))
                                         .withOptions(BatchWriteOptions.atomic(2))));
            assertEquals(BatchWriteResult.Status.ROLLED_BACK, duplicateKey.result().status());
            assertTrue(clients.syncForms().select(QuerySpec.of(form, byId("rollback"))).isEmpty());

            TableMetadata metadata = clients.syncOperator().metadata().readTable(tableName);
            assertTrue(metadata.findColumn("ID").isPresent());
            assertTrue(metadata.findColumn("NAME").isPresent());
        } finally {
            if (clients != null) {
                executeIgnoringFailure(clients, cleanupSql);
                clients.close();
            }
            source.assertAllConnectionsClosed();
        }
    }

    private static RdbDialect dialect(String name) {
        return switch (name) {
            case "mysql" -> RdbDialect.mysql();
            case "postgresql" -> RdbDialect.postgresql();
            case "oracle" -> RdbDialect.oracle();
            case "sqlserver" -> RdbDialect.sqlServer();
            default -> throw new IllegalArgumentException("unsupported certification dialect: " + name);
        };
    }

    private static DynamicForm form(String tableName) {
        return DynamicForm.builder("jdbcDialectCertification", tableName)
                          .addField(DynamicField.primaryKey("ID", "VARCHAR"))
                          .addField(DynamicField.of("NAME", "VARCHAR"))
                          .addField(DynamicField.of("AGE", "INTEGER"))
                          .build();
    }

    private static ConditionGroup byId(String id) {
        return ConditionGroup.and().where("ID", "=", id).build();
    }

    private static Map<String, Object> row(String id, String name, int age) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", id);
        row.put("NAME", name);
        row.put("AGE", age);
        return row;
    }

    /** 清理动作不应掩盖首次执行或连接中断后的主断言，认证结束时仍会尝试再次回收测试表。 */
    private static void executeIgnoringFailure(FlyingOrmClients clients, String cleanupSql) {
        try {
            clients.syncOperator().unsafeNativeSql(cleanupSql).execute();
        } catch (RuntimeException ignored) {
            // 表首次不存在或前一次失败留下半成品时都允许继续，后续的创建和主断言会给出真正失败原因。
        }
    }
}
