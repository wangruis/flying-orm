package com.flying.orm.rdb.form;

import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.rdb.codec.SqlTypedValue;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.internal.mapping.RepositoryUpsertValues;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class ScopedUpsertParameterLayoutTest {

    @Test
    void placesScopeBetweenInsertAndUpdateOnlyParametersOnlyOnMysql() {
        DynamicForm form = DynamicForm.builder("staged", "staged")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("insert_only", "VARCHAR"))
                .addField(DynamicField.of("update_only", "VARCHAR"))
                .addField(DynamicField.of("org_id", "VARCHAR")).build();
        RepositoryUpsertValues first = EntityValues.createUncached(StagedEntity.class)
                .repositoryUpsertValues(new StagedEntity(7L, "insert-1", "update-1"));
        ConditionGroup scope = ConditionGroup.and().where("org_id", "=", "A").build();
        for (RdbDialect dialect : dialects()) {
            FormBatchSqlRenderer renderer = renderer(dialect);
            BatchInsertPlan plan = renderer.upsertPlan(form, first, form, first, first, scope);
            List<Object> expected = dialect.name().equals("mysql")
                    ? List.of(7L, "insert-1", "A", "update-1")
                    : List.of(7L, "insert-1", "update-1", "A");
            assertEquals(expected, Arrays.asList(plan.firstParameters()), dialect.name());
            // 后续行和回执重算都走 parameters；Map 顺序不能决定 JDBC/R2DBC 绑定位置。
            Map<String, Object> reversed = new LinkedHashMap<>();
            reversed.put("update_only", "update-2");
            reversed.put("insert_only", "insert-2");
            reversed.put("id", 8L);
            assertEquals(dialect.name().equals("mysql")
                                 ? List.of(8L, "insert-2", "A", "update-2")
                                 : List.of(8L, "insert-2", "update-2", "A"),
                         Arrays.asList(plan.parameters(reversed, 1)), dialect.name());
            assertEquals(expected, Arrays.asList(plan.parameters(first, 0)), dialect.name());
            assertEquals(List.of(Long.class, String.class, String.class, String.class), plan.parameterTypes());
            assertTrue(plan.sql().contains("org_id"), plan.sql());
        }
    }

    @Test
    void noUpdateColumnsRemainTrueNoOpWithoutUnusedScopeParameters() {
        DynamicForm form = DynamicForm.builder("keys_only", "keys_only")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("org_id", "VARCHAR")).build();
        Map<String, Object> row = Map.of("id", 9L);
        for (RdbDialect dialect : dialects()) {
            FormBatchSqlRenderer renderer = renderer(dialect);
            BatchInsertPlan scoped = renderer.upsertPlan(form, row, form, row, row,
                    ConditionGroup.and().where("org_id", "=", "A").build());
            BatchInsertPlan plain = renderer.upsertPlan(form, row);
            assertEquals(plain.sql(), scoped.sql(), dialect.name());
            assertEquals(List.of(9L), Arrays.asList(scoped.firstParameters()));
            assertEquals(List.of(10L), Arrays.asList(scoped.parameters(Map.of("id", 10L), 1)));
            assertFalse(scoped.sql().contains("org_id"), scoped.sql());
        }
    }

    @Test
    void encryptedExactAndRelatedScopeUseTheConflictTargetAndStableBatchBindings() {
        DynamicForm form = DynamicForm.builder("protected_rows", "protected_rows")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("phone", "VARCHAR"))
                .encrypted("phone", EncryptedFieldDefinition.builder().build()).build();
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms()
                .addTerm(SqlTermHandler.relationExists("member-of", "memberships", "target", "owner", "role"))
                .build();
        DataScope scope = DataScope.where(ConditionGroup.and(conditions.terms())
                .add(ProtectedConditions.exact("phone", "old-phone"))
                .where("id", "member-of", 11L).build());
        Map<String, Object> logical = new LinkedHashMap<>();
        logical.put("id", 7L);
        logical.put("phone", "new-phone");
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            for (RdbDialect dialect : dialects()) {
                FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, dialect)
                        .withProtectedFields(runtime);
                FormScopeSupport scopes = new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), scope);
                DynamicForm physical = renderer.protection().physicalForm(form);
                FormPreparedWrite write = renderer.protection().writeOperation(form, physical, scope, null)
                        .prepare(logical);
                BatchInsertPlan plan = renderer.batchRenderer.upsertPlan(form, logical, physical, write.values(),
                        logical, scopes.prepareBatchScope(form, physical, scope).where());
                assertFalse(plan.sql().contains("old-phone"), plan.sql());
                assertFalse(plan.sql().contains(ProtectedConditions.EXACT), plan.sql());
                String target = dialect.name().equals("mysql")
                        ? dialect.schema().identifier(form.table()) : "target";
                assertTrue(plan.sql().contains(" = " + target + "." + dialect.schema().identifier("id")), plan.sql());
                if (!dialect.name().equals("mysql")) {
                    assertTrue(plan.sql().contains(dialect.schema().identifier("target_relation") + "."
                                                  + dialect.schema().identifier("owner")), plan.sql());
                }
                assertTrue(plan.firstParameters()[plan.firstParameters().length - 2] instanceof byte[]);
                assertEquals(11L, plan.firstParameters()[plan.firstParameters().length - 1]);
                Object[] replay = plan.parameters(write.values(), 1);
                Object[] first = plan.firstParameters();
                for (int index = 0; index < replay.length; index++) {
                    if (replay[index] instanceof byte[] bytes) {
                        assertArrayEquals((byte[]) first[index], bytes);
                    } else if (replay[index] instanceof SqlTypedValue typed) {
                        SqlTypedValue expected = assertInstanceOf(SqlTypedValue.class, first[index]);
                        assertEquals(expected.kind(), typed.kind());
                        assertArrayEquals(assertInstanceOf(byte[].class, expected.value()),
                                assertInstanceOf(byte[].class, typed.value()));
                    } else {
                        assertEquals(first[index], replay[index]);
                    }
                }
            }
        }
    }

    @Test
    void quotedRelationAliasCannotShadowTheUnquotedUpsertTarget() throws Exception {
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms()
                .addTerm(SqlTermHandler.relationExists("member-of", "memberships", "target", "owner", "role"))
                .build();
        DynamicForm form = DynamicForm.builder("accounts", "accounts")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR")).build();
        Map<String, Object> row = Map.of("id", 2L, "name", "changed");
        BatchInsertPlan plan = FormDataSqlRenderer.create(conditions, RdbDialect.postgresql()).batchRenderer
                .upsertPlan(form, row, form, row, row,
                        ConditionGroup.and(conditions.terms()).where("id", "member-of", 11L).build());
        // 执行的是 PostgreSQL 计划里的原样目标谓词，不用 H2 的不引用标识符模式冒充该别名边界。
        String predicate = plan.sql().substring(plan.sql().indexOf(" where ") + 7);
        try (var c = java.sql.DriverManager.getConnection(
                "jdbc:h2:mem:quoted_scope;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE")) {
            try (var s = c.createStatement()) {
                s.execute("create table accounts(id bigint primary key, name varchar(30))");
                s.execute("create table memberships(id bigint, owner bigint, role bigint)");
                s.execute("insert into accounts values(1,'allowed'),(2,'foreign')");
                s.execute("insert into memberships values(1,1,11)");
            }
            try (var s = c.prepareStatement("update accounts target set name='changed' where id=? and " + predicate)) {
                s.setLong(1, 1L);
                s.setLong(2, 11L);
                assertEquals(1, s.executeUpdate());
                s.setLong(1, 2L);
                org.junit.jupiter.api.Assertions.assertThrows(java.sql.SQLException.class, s::executeUpdate);
            }
            try (var s = c.createStatement(); var rows = s.executeQuery("select name from accounts where id=2")) {
                assertTrue(rows.next());
                assertEquals("foreign", rows.getString(1));
            }
        }
    }

    private static FormBatchSqlRenderer renderer(RdbDialect dialect) {
        return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), dialect).batchRenderer;
    }

    private static List<RdbDialect> dialects() {
        return List.of(RdbDialect.mysql(), RdbDialect.postgresql(), RdbDialect.h2(),
                       RdbDialect.sqlServer(), RdbDialect.oracle());
    }

    @TableName("staged")
    private record StagedEntity(@TableId(type = IdType.INPUT) Long id,
                               @TableField(updateStrategy = FieldStrategy.NEVER) String insertOnly,
                               @TableField(insertStrategy = FieldStrategy.NEVER) String updateOnly) {
    }
}
