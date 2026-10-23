package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FormWriteScopeReuseTest {

    @TestFactory
    Stream<DynamicTest> preservesWriteSqlAndTenantScopeAcrossDialects() {
        return Stream.of(RdbDialect.mysql(), RdbDialect.postgresql(), RdbDialect.oracle(),
                        RdbDialect.sqlServer(), RdbDialect.h2())
                .map(dialect -> DynamicTest.dynamicTest(dialect.toString(), () -> {
                    DynamicForm form = DynamicForm.builder("items", "items")
                            .addField(DynamicField.primaryKey("id", "BIGINT"))
                            .addField(DynamicField.of("label", "VARCHAR"))
                            .addField(DynamicField.of("tenant_id", "BIGINT"))
                            .tenant("tenant_id", TenantStrategy.MANUAL).build();
                    FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                            SqlRenderer.builder().addDefaultTerms().build(), dialect);
                    FormOperationPlanner planner = new FormOperationPlanner(renderer,
                            new FormScopeSupport(renderer, StructuredConditionResolver.defaults(),
                                    DataScope.tenant("tenant_id", 11L)), SqlExecutionOptions.unlimited());
                    ConditionGroup where = ConditionGroup.and().where("id", "=", 7L).build();
                    DataScope requestScope = DataScope.where(
                            ConditionGroup.and().where("id", "<", 100L).build());
                    WriteSpec insert = WriteSpec.insert(form, Map.of("id", 7L, "label", "value", "tenant_id", 11L));
                    WriteSpec update = WriteSpec.update(form, Map.of("label", "value"), where)
                            .withScope(requestScope);
                    WriteSpec delete = WriteSpec.delete(form, where).withScope(requestScope);
                    FieldUsePolicy policy = FieldUsePolicy.unrestricted();
                    QueryShapeLimits limits = QueryShapeLimits.defaults();
                    List<FormOperationPlanner.PlannedWrite> plain = List.of(planner.insert(insert),
                            planner.update(update), planner.delete(delete), planner.physicalDelete(delete));
                    List<FormOperationPlanner.PlannedWrite> governed = List.of(
                            planner.write(insert, FormOperationPlanner.WriteAction.INSERT, true, policy, limits),
                            planner.write(update, FormOperationPlanner.WriteAction.UPDATE, true, policy, limits),
                            planner.write(delete, FormOperationPlanner.WriteAction.DELETE, true, policy, limits),
                            planner.write(delete, FormOperationPlanner.WriteAction.PHYSICAL_DELETE, true, policy, limits));
                    for (int index = 0; index < plain.size(); index++) {
                        assertEquals(plain.get(index).request().sql(), governed.get(index).request().sql());
                        assertEquals(plain.get(index).request().parameters(), governed.get(index).request().parameters());
                    }
                    assertEquals(List.of("value", 7L, 11L, 100L), plain.get(1).request().parameters());
                    assertEquals(List.of(7L, 11L, 100L), plain.get(2).request().parameters());

                    WriteSpec scopeUpdate = WriteSpec.update(form, Map.of("label", "value"),
                            ConditionGroup.and().build()).withScope(requestScope);
                    WriteSpec scopeDelete = WriteSpec.delete(form, ConditionGroup.and().build()).withScope(requestScope);
                    assertEquals(List.of("value", 11L, 100L), planner.update(scopeUpdate).request().parameters());
                    assertEquals(List.of(11L, 100L), planner.delete(scopeDelete).request().parameters());
                    assertEquals(List.of(11L, 100L), planner.physicalDelete(scopeDelete).request().parameters());

                    DataScope conflict = DataScope.tenant("tenant_id", 12L);
                    WriteSpec unsafeUpdate = WriteSpec.update(form, Map.of("label", "value"),
                            ConditionGroup.and().build()).withScope(conflict);
                    WriteSpec unsafeDelete = WriteSpec.delete(form, ConditionGroup.and().build()).withScope(conflict);
                    for (Runnable action : List.<Runnable>of(() -> planner.update(unsafeUpdate),
                            () -> planner.delete(unsafeDelete), () -> planner.physicalDelete(unsafeDelete),
                            () -> planner.write(unsafeUpdate, FormOperationPlanner.WriteAction.UPDATE, true, policy, limits),
                            () -> planner.write(unsafeDelete, FormOperationPlanner.WriteAction.DELETE, true, policy, limits),
                            () -> planner.write(unsafeDelete, FormOperationPlanner.WriteAction.PHYSICAL_DELETE, true, policy, limits))) {
                        assertEquals("conflicting tenant scope values",
                                assertThrows(IllegalArgumentException.class, action::run).getMessage());
                    }
                }));
    }

    @Test
    void emptyBusinessConditionsDoNotInventPredicatesOrDiscardAnExplicitVersion() {
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("label", "VARCHAR"))
                .addField(DynamicField.of("version", "INTEGER")).build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        ConditionGroup empty = ConditionGroup.or().add(ConditionGroup.and().build())
                .add(ConditionGroup.or().build()).build();
        SqlRequest update = renderer.update(form, Map.of("label", "new"), empty);
        assertEquals("update \"items\" set \"label\" = ?", update.sql());
        assertEquals(List.of("new"), update.parameters());
        assertEquals("delete from \"items\"", renderer.delete(form, empty).sql());
        for (OptimisticLockOptions lock : List.of(OptimisticLockOptions.increment("version", 3),
                OptimisticLockOptions.assign("version", 3, 4))) {
            SqlRequest locked = renderer.update(form, Map.of("label", "new"), empty, lock);
            assertEquals("update \"items\" set \"label\" = ?, \"version\" = "
                    + (lock.nextValue() == null ? "\"version\" + 1" : "?")
                    + " where \"version\" = ?", locked.sql());
            assertEquals(lock.nextValue() == null ? List.of("new", 3) : List.of("new", 4, 3), locked.parameters());
            SqlRequest delete = renderer.delete(form, empty, lock);
            assertEquals("delete from \"items\" where \"version\" = ?", delete.sql());
            assertEquals(List.of(3), delete.parameters());
        }
    }
}
