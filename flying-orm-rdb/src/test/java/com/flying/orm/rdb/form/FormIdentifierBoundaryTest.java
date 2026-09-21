package com.flying.orm.rdb.form;

import com.flying.orm.core.sql.render.SqlIdentifiers;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import com.flying.orm.rdb.dialect.DialectFeature;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.plan.StructuralPlanCaches;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FormIdentifierBoundaryTest {

    @Test
    void cacheMetadataDoesNotReparseLiteralRelationNamesAsLegacyTablePaths() {
        DynamicForm form = DynamicForm.relationalBuilder("archive",
                com.flying.orm.core.metadata.RelationIdentity.table("orders.archive.history"))
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        var renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        var where = ConditionGroup.and().where("id", "=", 1L).build();
        for (var request : List.of(renderer.select(form, where),
                renderer.insert(form, Map.of("id", 2L)),
                renderer.update(form, Map.of("id", 2L), where), renderer.delete(form, where))) {
            assertTrue(request.sql().contains("\"orders.archive.history\""), request.sql());
        }
    }

    @Test
    void registeredRelationsKeepOrdinaryConditionPlansCacheable() {
        RdbDialect dialect = RdbDialect.postgresql();
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms()
                .addTerm(SqlTermHandler.relationExists("member-of", "membership", "m", "owner_id", "group_id"))
                .build().withIdentifierRenderer(dialect.schema()::identifier);
        FormSqlRenderSupport support = new FormSqlRenderSupport(conditions, dialect.json(), dialect.name(), true,
                dialect.schema()::identifier, StructuralPlanCaches.create(OrmCachePolicy.safeDefaults()));
        DynamicForm form = DynamicForm.builder("users", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        assertTrue(support.condition(form, ConditionGroup.and().where("id", "=", 1L).build()).cacheable());
        assertEquals("\"id\" = ?",
                support.condition(form, ConditionGroup.and().where("id", "=", 2L).build()).sql());
    }

    @Test
    void joinRelationTermsUseEachSourceQualifierAndAvoidShadowing() {
        DynamicForm form = DynamicForm.builder("users", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        DynamicForm otherForm = DynamicForm.builder("owners", "owners")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms()
                .addTerm(SqlTermHandler.relationExists("by-root", "membership", "t0", "owner_id", "group_id"))
                .addTerm(SqlTermHandler.relationNotExists("by-other", "membership", "t1", "owner_id", "group_id"))
                .addTerm(SqlTermHandler.relationExists("by-scope", "membership", "users", "owner_id", "group_id"))
                .build();
        var builder = com.flying.orm.core.join.JoinQuerySpec.builder(form);
        var root = builder.root();
        var other = builder.join(com.flying.orm.core.join.JoinType.INNER, otherForm, root, "id", "id");
        var spec = builder.select(root, "id").select(other, "id")
                .where(root, ConditionGroup.and(conditions.terms()).where("id", "by-root", 1L).build())
                .where(other, ConditionGroup.and(conditions.terms()).where("id", "by-other", 2L).build())
                .scope(root, com.flying.orm.core.scope.DataScope.where(
                        ConditionGroup.and(conditions.terms()).where("id", "by-scope", 3L).build())).build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, RdbDialect.postgresql());
        var planner = new JoinQueryPlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(),
                        com.flying.orm.core.scope.DataScope.none()),
                com.flying.orm.rdb.execution.SqlExecutionOptions.safeDefaults());
        String sql = planner.plan(spec, null).request().sql();
        assertTrue(sql.contains("\"t0_relation\".\"owner_id\" = \"t0\".\"id\""), sql);
        assertTrue(sql.contains("\"t1_relation\".\"owner_id\" = \"t1\".\"id\""), sql);
        assertTrue(sql.contains("\"users_relation\".\"owner_id\" = \"users\".\"id\""), sql);
    }

    @Test
    void containsRelationTermsAvoidTheBusinessAlias() {
        DynamicForm form = DynamicForm.builder("users", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", com.flying.orm.core.protection.EncryptedFieldDefinition.builder()
                        .searchModes(com.flying.orm.core.protection.EncryptedSearchMode.CONTAINS).build()).build();
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms()
                .addTerm(SqlTermHandler.relationExists(
                        "member-of", "membership", "fop_business", "owner_id", "group_id")).build();
        try (var runtime = com.flying.orm.rdb.protection.ProtectedFieldRuntime.create(
                com.flying.orm.rdb.protection.ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, RdbDialect.postgresql())
                    .withProtectedFields(runtime);
            var planner = new FormOperationPlanner(renderer,
                    new FormScopeSupport(renderer, StructuredConditionResolver.defaults(),
                            com.flying.orm.core.scope.DataScope.none()),
                    com.flying.orm.rdb.execution.SqlExecutionOptions.safeDefaults());
            var where = ConditionGroup.and(conditions.terms())
                    .add(com.flying.orm.rdb.protection.ProtectedConditions.contains("secret", "abc"))
                    .where("id", "member-of", 7L).build();
            String sql = planner.select(com.flying.orm.rdb.form.spec.QuerySpec.of(form, where)).request().sql();
            assertTrue(sql.contains("\"fop_business_relation\".\"owner_id\" = fop_business.\"id\""), sql);
        }
    }

    @Test
    void relationPredicatesQualifyTheOuterFieldAcrossReadAndWrite() {
        DynamicForm form = DynamicForm.builder("users", "users")
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR"))
                .build();
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer(), RdbDialect.h2())) {
            SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms()
                    .addTerm(SqlTermHandler.relationExists(
                            "member-of", "membership", "m", "owner_id", "group_id"))
                    .addTerm(SqlTermHandler.relationNotExists(
                            "not-member-of", "membership", "m", "owner_id", "group_id"))
                    .build();
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, dialect);
            String outerField = dialect.schema().identifier("users") + "."
                    + dialect.schema().identifier("id");
            for (String operator : List.of("member-of", "not-member-of")) {
                ConditionGroup where = ConditionGroup.and(conditions.terms())
                        .where("id", operator, 7L).build();
                for (var request : List.of(renderer.select(form, where),
                        renderer.update(form, Map.of("name", "selected"), where),
                        renderer.delete(form, where))) {
                    assertTrue(request.sql().contains(" = " + outerField + " and "), request.sql());
                }
            }
        }
    }

    @Test
    void delegatesIdentifierValidationToTheFinalDialectRendererExactlyOnce() {
        RdbDialect dialect = RdbDialect.postgresql();
        AtomicInteger calls = new AtomicInteger();
        UnaryOperator<String> identifiers = name -> {
            calls.incrementAndGet();
            return dialect.schema().identifier(
                    SqlIdentifiers.requireIdentifier(name, "test form identifier"));
        };
        FormSqlRenderSupport support = new FormSqlRenderSupport(
                SqlRenderer.builder().addDefaultTerms().build().withIdentifierRenderer(identifiers),
                dialect.json(),
                dialect.name(),
                dialect.supports(DialectFeature.NATIVE_BOOLEAN),
                identifiers,
                StructuralPlanCaches.create(OrmCachePolicy.safeDefaults()));

        assertThrows(IllegalArgumentException.class, () -> support.identifier("unsafe;drop"));
        assertEquals(1, calls.get());
    }
}
