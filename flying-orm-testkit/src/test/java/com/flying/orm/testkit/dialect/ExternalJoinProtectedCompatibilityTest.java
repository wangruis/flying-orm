package com.flying.orm.testkit.dialect;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.spi.ConnectionFactories;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 用四种真实数据库同时认证轻量 JOIN 与显式字段保护的 JDBC/R2DBC 公共入口。
 *
 * <p>测试只在同一数据库的 JDBC、R2DBC URL 都已配置时运行。每个场景都从统一客户端装配出两条原生执行链，
 * 真实验证 {@code join/leftJoin/rightJoin}、密文存储、精确/后缀/包含搜索、默认脱敏与可信完整展示，避免新增能力
 * 只在 H2 或 SQL 渲染单测中成立。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
class ExternalJoinProtectedCompatibilityTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Test
    void verifiesMysqlJoinAndProtectedFieldsWhenConfigured() {
        runIfConfigured("mysql", RdbDialect.mysql());
    }

    @Test
    void verifiesPostgresqlJoinAndProtectedFieldsWhenConfigured() {
        runIfConfigured("postgresql", RdbDialect.postgresql());
    }

    @Test
    void verifiesOracleJoinAndProtectedFieldsWhenConfigured() {
        runIfConfigured("oracle", RdbDialect.oracle());
    }

    @Test
    void verifiesSqlServerJoinAndProtectedFieldsWhenConfigured() {
        runIfConfigured("sqlserver", RdbDialect.sqlServer());
    }

    private static void runIfConfigured(String name, RdbDialect dialect) {
        String reactiveUrl = System.getProperty("flying.orm.compat." + name + ".url");
        String jdbcPrefix = "flying.orm.compat." + name + ".jdbc";
        String jdbcUrl = System.getProperty(jdbcPrefix + ".url");
        Assumptions.assumeTrue(reactiveUrl != null && !reactiveUrl.isBlank()
                                       && jdbcUrl != null && !jdbcUrl.isBlank(),
                               name + " JDBC and R2DBC URLs are not configured");

        TrackingDriverManagerDataSource source = new TrackingDriverManagerDataSource(
                jdbcUrl.trim(), System.getProperty(jdbcPrefix + ".user"),
                System.getProperty(jdbcPrefix + ".password"));
        ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(41));
        FlyingOrmClients clients = null;
        DynamicForm root = joinRoot();
        DynamicForm item = joinItem();
        DynamicForm protectedForm = protectedForm();
        List<String> cleanupTables = List.of(
                ProtectedContainsLayout.resolve(protectedForm).orElseThrow().table().table(),
                protectedForm.table(), item.table(), root.table());
        try {
            clients = FlyingOrmClients.builder(source, ConnectionFactories.get(reactiveUrl.trim()))
                                      .configuredDialect(name)
                                      .protectedFields(keys)
                                      .build();
            cleanup(clients, dialect, cleanupTables);
            clients.syncSchema().createTable(root);
            clients.syncSchema().createTable(item);
            clients.syncSchema().createTable(protectedForm);

            verifyJoin(clients, root, item);
            verifyProtectedFields(clients, protectedForm);
        } finally {
            if (clients != null) {
                cleanup(clients, dialect, cleanupTables);
                clients.close();
            } else {
                keys.close();
            }
            source.assertAllConnectionsClosed();
        }
    }

    private static void verifyJoin(FlyingOrmClients clients, DynamicForm root, DynamicForm item) {
        clients.syncForms().insert(WriteSpec.insert(root, row("ID", 1L, "NAME", "Alice")));
        clients.syncForms().insert(WriteSpec.insert(root, row("ID", 2L, "NAME", "Bob")));
        clients.syncForms().insert(WriteSpec.insert(item, row("ID", 10L, "ROOT_ID", 1L, "LABEL", "owned")));
        clients.syncForms().insert(WriteSpec.insert(item, row("ID", 20L, "ROOT_ID", 3L, "LABEL", "orphan")));

        List<DynamicRow> reactiveInner = clients.operator().dml().joinQuery(root)
                                                .join(item, "ID", "ROOT_ID")
                                                .selectAs(root, "NAME", "rootName")
                                                .selectAs(item, "LABEL", "itemLabel")
                                                .executeRows().collectList().block(TIMEOUT);
        List<DynamicRow> reactiveLeft = clients.operator().dml().joinQuery(root)
                                               .leftJoin(item, "ID", "ROOT_ID")
                                               .selectAs(root, "NAME", "rootName")
                                               .selectAs(item, "LABEL", "itemLabel")
                                               .executeRows().collectList().block(TIMEOUT);
        List<DynamicRow> reactiveRight = clients.operator().dml().joinQuery(root)
                                                .rightJoin(item, "ID", "ROOT_ID")
                                                .selectAs(root, "NAME", "rootName")
                                                .selectAs(item, "LABEL", "itemLabel")
                                                .executeRows().collectList().block(TIMEOUT);

        assertJoinResults(reactiveInner, reactiveLeft, reactiveRight);
        assertJoinResults(
                clients.syncOperator().dml().joinQuery(root)
                       .join(item, "ID", "ROOT_ID")
                       .selectAs(root, "NAME", "rootName")
                       .selectAs(item, "LABEL", "itemLabel").executeRows(),
                clients.syncOperator().dml().joinQuery(root)
                       .leftJoin(item, "ID", "ROOT_ID")
                       .selectAs(root, "NAME", "rootName")
                       .selectAs(item, "LABEL", "itemLabel").executeRows(),
                clients.syncOperator().dml().joinQuery(root)
                       .rightJoin(item, "ID", "ROOT_ID")
                       .selectAs(root, "NAME", "rootName")
                       .selectAs(item, "LABEL", "itemLabel").executeRows());
    }

    private static void assertJoinResults(List<DynamicRow> inner,
                                          List<DynamicRow> left,
                                          List<DynamicRow> right) {
        assertEquals(1, inner.size());
        assertEquals("Alice", inner.getFirst().get("rootName"));
        assertEquals("owned", inner.getFirst().get("itemLabel"));
        assertEquals(2, left.size());
        DynamicRow bob = left.stream().filter(row -> "Bob".equals(row.get("rootName"))).findFirst().orElseThrow();
        assertNull(bob.get("itemLabel"));
        assertEquals(2, right.size());
        DynamicRow orphan = right.stream()
                                  .filter(row -> "orphan".equals(row.get("itemLabel")))
                                  .findFirst().orElseThrow();
        assertNull(orphan.get("rootName"));
    }

    private static void verifyProtectedFields(FlyingOrmClients clients, DynamicForm form) {
        String first = "13800138000";
        String second = "13900139000";
        assertEquals(1L, clients.syncForms().insert(WriteSpec.insert(
                form, row("ID", 101L, "CONTACT", first))));
        assertEquals(1L, clients.forms().insert(WriteSpec.insert(
                form, row("ID", 102L, "CONTACT", second))).block(TIMEOUT));

        QuerySpec exact = query(form, ProtectedConditions.exact("CONTACT", first));
        List<DynamicRow> masked = clients.syncForms().select(exact);
        assertEquals(1, masked.size());
        assertEquals("13*******00", masked.getFirst().get("CONTACT"));
        assertEquals(first, clients.syncForms().select(exact.showSensitive()).getFirst().get("CONTACT"));

        List<DynamicRow> suffix = clients.forms().select(query(
                form, ProtectedConditions.suffix("CONTACT", "8000"))).collectList().block(TIMEOUT);
        assertEquals(List.of(101L), ids(suffix));
        List<DynamicRow> contains = clients.forms().select(query(
                form, ProtectedConditions.contains("CONTACT", "1380"))).collectList().block(TIMEOUT);
        assertEquals(List.of(101L), ids(contains));
    }

    private static QuerySpec query(DynamicForm form, com.flying.orm.core.condition.TermCondition term) {
        return QuerySpec.of(form, ConditionGroup.and().add(term).build());
    }

    private static List<Long> ids(List<DynamicRow> rows) {
        return rows.stream().map(row -> ((Number) row.get("ID")).longValue()).sorted().toList();
    }

    private static DynamicForm joinRoot() {
        return DynamicForm.builder("certJoinRoot", "FOP_CERT_JOIN_ROOT")
                          .addField(DynamicField.primaryKey("ID", "BIGINT"))
                          .addField(DynamicField.of("NAME", "VARCHAR"))
                          .build();
    }

    private static DynamicForm joinItem() {
        return DynamicForm.builder("certJoinItem", "FOP_CERT_JOIN_ITEM")
                          .addField(DynamicField.primaryKey("ID", "BIGINT"))
                          .addField(DynamicField.of("ROOT_ID", "BIGINT"))
                          .addField(DynamicField.of("LABEL", "VARCHAR"))
                          .build();
    }

    private static DynamicForm protectedForm() {
        return DynamicForm.builder("certProtected", "FOP_CERT_PROTECTED")
                          .addField(DynamicField.primaryKey("ID", "BIGINT"))
                          .addField(DynamicField.of("CONTACT", "VARCHAR"))
                          .encrypted("CONTACT", EncryptedFieldDefinition.builder()
                                                                         .searchModes(
                                                                                 EncryptedSearchMode.EXACT,
                                                                                 EncryptedSearchMode.SUFFIX,
                                                                                 EncryptedSearchMode.CONTAINS)
                                                                         .normalizer("digits")
                                                                         .suffixLengths(4)
                                                                         .build())
                          .masked("CONTACT", MaskedFieldDefinition.builder("partial")
                                                                   .prefix(2)
                                                                   .suffix(2)
                                                                   .build())
                          .build();
    }

    private static void cleanup(FlyingOrmClients clients, RdbDialect dialect, List<String> tables) {
        for (String table : tables) {
            String identifier = dialect.schema().identifier(table);
            String sql = "oracle".equals(dialect.name())
                    ? "drop table " + identifier
                    : "drop table if exists " + identifier;
            try {
                clients.syncOperator().unsafeNativeSql(sql).execute();
            } catch (RuntimeException ignored) {
                // Oracle 没有 DROP TABLE IF EXISTS；清理不存在的认证表时允许继续，真实建表或查询失败仍会直接暴露。
            }
        }
    }

    private static Map<String, Object> row(Object... pairs) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int index = 0; index < pairs.length; index += 2) {
            values.put((String) pairs[index], pairs[index + 1]);
        }
        return values;
    }

    private static byte[] key(int seed) {
        byte[] key = new byte[32];
        Arrays.fill(key, (byte) seed);
        return key;
    }
}
