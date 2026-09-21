package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.WriteSpec;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FormWriteScopeReuseTest {

    @TestFactory
    Stream<DynamicTest> preservesWriteSqlAndBusinessPredicateRejectionAcrossDialects() {
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
                                    DataScope.tenant("tenant_id", 11L)), SqlExecutionOptions.safeDefaults());
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
                            planner.insertGoverned(insert, policy, limits).plan(),
                            planner.updateGoverned(update, policy, limits).plan(),
                            planner.deleteGoverned(delete, policy, limits).plan(),
                            planner.physicalDeleteGoverned(delete, policy, limits).plan());
                    for (int index = 0; index < plain.size(); index++) {
                        assertEquals(plain.get(index).request().sql(), governed.get(index).request().sql());
                        assertEquals(plain.get(index).request().parameters(), governed.get(index).request().parameters());
                    }
                    assertEquals(List.of("value", 7L, 11L, 100L), plain.get(1).request().parameters());
                    assertEquals(List.of(7L, 11L, 100L), plain.get(2).request().parameters());

                    DataScope conflict = DataScope.tenant("tenant_id", 12L);
                    WriteSpec unsafeUpdate = WriteSpec.update(form, Map.of("label", "value"),
                            ConditionGroup.and().build()).withScope(conflict);
                    WriteSpec unsafeDelete = WriteSpec.delete(form, ConditionGroup.and().build()).withScope(conflict);
                    for (Runnable action : List.<Runnable>of(() -> planner.update(unsafeUpdate),
                            () -> planner.delete(unsafeDelete), () -> planner.physicalDelete(unsafeDelete),
                            () -> planner.updateGoverned(unsafeUpdate, policy, limits),
                            () -> planner.deleteGoverned(unsafeDelete, policy, limits),
                            () -> planner.physicalDeleteGoverned(unsafeDelete, policy, limits))) {
                        assertEquals("write business where condition must not be empty",
                                assertThrows(IllegalArgumentException.class, action::run).getMessage());
                    }
                }));
    }
}
