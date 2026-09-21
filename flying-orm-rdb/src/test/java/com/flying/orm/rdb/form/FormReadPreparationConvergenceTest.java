package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorSort;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FormReadPreparationConvergenceTest {

    @Test
    void ordinaryProtectedSearchRetainsScopeAndPageShapes() {
        try (Fixture fixture = new Fixture()) {
            ConditionGroup where = ConditionGroup.and()
                    .add(ProtectedConditions.exact("secret", "alphabet"))
                    .add(ProtectedConditions.suffix("secret", "bet")).build();
            QuerySpec spec = QuerySpec.of(fixture.form, where)
                    .withScope(DataScope.where(ConditionGroup.and().where("tenant_id", "=", 9L).build()));
            var select = fixture.planner.select(spec);
            var page = fixture.planner.page(spec, new PageQuery(1, 2, List.of()));
            var cursor = fixture.planner.cursorPage(spec, CursorPageQuery.first(2, CursorSort.asc("id")));
            assertNull(select.containsQuery());
            assertNotNull(page.countRequest());
            assertTrue(select.request().parameters().contains(9L));
            assertTrue(cursor.request().parameters().contains(3));
            assertFalse(select.request().parameters().contains("alphabet"));
            assertFalse(select.request().parameters().contains("bet"));
        }
    }

    @Test
    void containsOmitsOffsetCountAndKeepsCandidateLimit() {
        try (Fixture fixture = new Fixture()) {
            QuerySpec spec = QuerySpec.of(fixture.form, ConditionGroup.and()
                    .add(ProtectedConditions.contains("secret", "alpha")).build());
            var select = fixture.planner.select(spec);
            var page = fixture.planner.page(spec, new PageQuery(1, 2, List.of()));
            assertNotNull(select.containsQuery());
            assertNull(page.countRequest());
            assertNotNull(page.containsQuery());
            assertTrue(page.dataRequest().parameters().contains(
                    ProtectedContainsResultSupport.DEFAULT_CANDIDATE_LIMIT + 1));
        }
    }

    @Test
    void containsStructureErrorsPrecedeInvalidOrdinaryEncryptedComparison() {
        try (Fixture fixture = new Fixture()) {
            ConditionGroup nested = ConditionGroup.and().where("secret", "=", "invalid")
                    .add(ConditionGroup.or().add(ProtectedConditions.contains("secret", "alpha")).build())
                    .build();
            assertEquals("protected contains search requires a top-level AND condition",
                    assertThrows(IllegalArgumentException.class,
                            () -> fixture.planner.select(QuerySpec.of(fixture.form, nested))).getMessage());
            ConditionGroup multiple = ConditionGroup.and().where("secret", "=", "invalid")
                    .add(ProtectedConditions.contains("secret", "alpha"))
                    .add(ProtectedConditions.contains("secret", "beta")).build();
            assertEquals("protected contains search supports one field per query",
                    assertThrows(IllegalArgumentException.class,
                            () -> fixture.planner.select(QuerySpec.of(fixture.form, multiple))).getMessage());
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final DynamicForm form = DynamicForm.builder("read-preparation", "read_preparation")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .addField(DynamicField.of("tenant_id", "BIGINT"))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.EXACT, EncryptedSearchMode.SUFFIX,
                                EncryptedSearchMode.CONTAINS).suffixLengths(3).build()).build();
        private final ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]));
        private final FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql())
                .withProtectedFields(runtime);
        private final FormOperationPlanner planner = new FormOperationPlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.safeDefaults());

        @Override
        public void close() {
            runtime.close();
        }
    }
}
