package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JoinProtectedScopeTest {

    @Test
    void keepsPagedSqlProtectedReadabilityAndEscapingScopeContracts() {
        DynamicForm accounts = DynamicForm.builder("accounts-page", "accounts_page")
                                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                                          .addField(DynamicField.of("phone", "VARCHAR"))
                                          .encrypted("phone", EncryptedFieldDefinition.builder().build())
                                          .build();
        DynamicForm owners = DynamicForm.builder("owners-page", "owners_page")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("account_id", "BIGINT"))
                                        .build();
        ConditionGroup protectedWhere = ConditionGroup.and()
                                                        .add(ProtectedConditions.exact("phone", "13800138000"))
                                                        .build();
        DataScope protectedScope = DataScope.where(protectedWhere).withFields(FieldScope.readable("id"));
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(accounts);
        JoinSource account = builder.root();
        JoinSource owner = builder.join(JoinType.INNER, owners, account, "id", "account_id");
        JoinQuerySpec spec = builder.scope(account, protectedScope)
                                    .select(account, "id")
                                    .select(owner, "id")
                                    .orderBy(account, "id", PageSort.Direction.ASC)
                                    .build();

        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql())
                    .withProtectedFields(runtime);
            JoinQueryPlanner planner = new JoinQueryPlanner(
                    renderer,
                    new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                    SqlExecutionOptions.safeDefaults());

            JoinQueryPlanner.PlannedJoinPage plan = planner.page(spec, PageQuery.of(2, 25), null);

            assertTrue(plan.countRequest().sql().startsWith("select count(*) as total"));
            assertFalse(plan.countRequest().sql().contains(ProtectedConditions.EXACT));
            assertFalse(plan.dataRequest().sql().contains(ProtectedConditions.EXACT));
            assertTrue(plan.dataRequest().sql().contains(" order by \"t0\".\"id\" asc"),
                       plan.dataRequest().sql());
            assertEquals(plan.countRequest().parameters().size() + 2, plan.dataRequest().parameters().size());
            assertArrayEquals((byte[]) plan.countRequest().parameters().getFirst(),
                              (byte[]) plan.dataRequest().parameters().getFirst());
            assertThrows(UnsupportedOperationException.class,
                         () -> plan.scopes().put(account, DataScope.none()));

            JoinQuerySpec.Builder deniedBuilder = JoinQuerySpec.builder(accounts);
            JoinSource deniedRoot = deniedBuilder.root();
            JoinQuerySpec denied = deniedBuilder.scope(deniedRoot, protectedScope)
                                                     .select(deniedRoot, "phone")
                                                     .build();
            assertThrows(IllegalArgumentException.class, () -> planner.plan(denied, null));
        }
    }

    @Test
    void rewritesProtectedConditionsDeclaredBySourceDataScope() {
        DynamicForm form = DynamicForm.builder("accounts", "accounts")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("phone", "VARCHAR"))
                                      .encrypted("phone", EncryptedFieldDefinition.builder().build())
                                      .build();
        ConditionGroup scopeWhere = ConditionGroup.and()
                                                  .add(ProtectedConditions.exact("phone", "13800138000"))
                                                  .build();
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form);
        JoinSource root = builder.root();
        JoinQuerySpec spec = builder.scope(root, DataScope.where(scopeWhere))
                                    .select(root, "id")
                                    .build();

        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql())
                    .withProtectedFields(runtime);
            JoinQueryPlanner planner = new JoinQueryPlanner(
                    renderer,
                    new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                    SqlExecutionOptions.safeDefaults());

            JoinQueryPlanner.PlannedJoin plan = assertDoesNotThrow(() -> planner.plan(spec, null));

            assertFalse(plan.request().sql().contains(ProtectedConditions.EXACT));
        }
    }

    @Test
    void rewritesAProtectedDataScopeFieldThatIsHiddenFromTheResult() {
        DynamicForm form = DynamicForm.builder("accounts", "accounts")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("phone", "VARCHAR"))
                                      .encrypted("phone", EncryptedFieldDefinition.builder().build())
                                      .build();
        ConditionGroup scopeWhere = ConditionGroup.and()
                                                  .add(ProtectedConditions.exact("phone", "13800138000"))
                                                  .build();
        DataScope scope = DataScope.where(scopeWhere).withFields(FieldScope.readable("id"));

        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql())
                    .withProtectedFields(runtime);
            FormOperationPlanner planner = new FormOperationPlanner(
                    renderer,
                    new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                    SqlExecutionOptions.safeDefaults());

            FormOperationPlanner.PlannedQuery plan = assertDoesNotThrow(() -> planner.select(
                    QuerySpec.of(form, ConditionGroup.and().build()).withScope(scope)));

            assertFalse(plan.request().sql().contains(ProtectedConditions.EXACT));
        }
    }
}
