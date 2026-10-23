package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.internal.binding.SqlNullParameter;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.WriteSpec;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class UpdateExpressionContractTest {
    @Test
    void cachedAndUncachedMixedAssignmentsKeepTheSameSqlAndBindings() {
        DynamicForm form = DynamicForm.builder("mixed_update", "mixed_update")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("amount", "DECIMAL"))
                .addField(DynamicField.of("quantity", "INTEGER"))
                .addField(DynamicField.of("note", "VARCHAR"))
                .addField(DynamicField.of("version", "BIGINT")).build();
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            for (boolean cached : List.of(false, true)) {
                com.flying.orm.rdb.cache.OrmCachePolicy policy = cached
                        ? com.flying.orm.rdb.cache.OrmCachePolicy.safeDefaults()
                        : com.flying.orm.rdb.cache.OrmCachePolicy.builder()
                                .sqlPlans(com.flying.orm.rdb.cache.CacheRegionPolicy.disabled()).build();
                FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                        SqlRenderer.builder().addDefaultTerms().build(), dialect,
                        com.flying.orm.rdb.internal.plan.StructuralPlanCaches.create(policy),
                        CacheRegionPolicy.sqlPlanDefaults(), ProtectedFieldRuntime.withoutKeys(), List.of());
                SqlRequest previous = null;
                for (int value : List.of(2, 9)) {
                    Map<String, Object> values = new LinkedHashMap<>();
                    values.put("AMOUNT", ValueExpression.value(value).add(ValueExpression.value(3)));
                    values.put("quantity", UpdateDelta.increment(4));
                    values.put("note", ValueExpression.value(null));
                    SqlRequest request = renderer.update(form, values, where(1),
                            OptimisticLockOptions.assign("VERSION", 7L, 8L));
                    java.util.function.Function<String, String> quote = dialect.schema()::identifier;
                    assertEquals("update " + quote.apply("mixed_update") + " set "
                            + quote.apply("amount") + " = (? + ?), "
                            + quote.apply("quantity") + " = " + quote.apply("quantity") + " + ?, "
                            + quote.apply("note") + " = ?, " + quote.apply("version") + " = ? where "
                            + quote.apply("id") + " = ? and " + quote.apply("version") + " = ?", request.sql());
                    assertEquals(List.of(value, 3, java.math.BigDecimal.valueOf(4)),
                            request.parameters().subList(0, 3));
                    assertEquals(String.class, assertInstanceOf(SqlNullParameter.class,
                            request.parameters().get(3)).javaType());
                    assertEquals(List.of(8L, 1L, 7L), request.parameters().subList(4, 7));
                    if (previous != null) {
                        if (cached) assertSame(previous.statement(), request.statement());
                        else assertNotSame(previous.statement(), request.statement());
                    }
                    previous = request;
                }
            }
        }
    }

    @Test
    void assignmentShapesSeparatePlainDeltaExpressionAndLockModes() {
        FormDataSqlRenderer renderer = renderer();
        SqlRequest plain = renderer.update(FORM, Map.of("quantity", 2), where(1));
        SqlRequest next = renderer.update(FORM, Map.of("QUANTITY", 5), where(2));
        assertSame(plain.statement(), next.statement());
        assertEquals(List.of(5, 2L), next.parameters());
        SqlRequest delta = renderer.update(FORM, Map.of("quantity", UpdateDelta.increment(2)), where(1));
        SqlRequest expression = renderer.update(FORM,
                Map.of("quantity", ValueExpression.field("quantity").multiply(ValueExpression.value(2))), where(1));
        assertNotSame(plain.statement(), delta.statement());
        assertNotSame(delta.statement(), expression.statement());
        assertTrue(plain.sql().contains("\"quantity\" = ?"));
        assertTrue(delta.sql().contains("\"quantity\" = \"quantity\" + ?"));
        assertTrue(expression.sql().contains("\"quantity\" = (\"quantity\" * ?)"));
        SqlRequest increment = renderer.update(FORM, Map.of("quantity", 2), where(1),
                OptimisticLockOptions.increment("version", 7L));
        SqlRequest assign = renderer.update(FORM, Map.of("quantity", 2), where(1),
                OptimisticLockOptions.assign("version", 7L, 8L));
        assertNotSame(increment.statement(), assign.statement());
        assertEquals(List.of(2, 1L, 7L), increment.parameters());
        assertEquals(List.of(2, 8L, 1L, 7L), assign.parameters());
    }

    private static final DynamicForm FORM = DynamicForm.builder("sales", "sales")
            .addField(DynamicField.of("id", "BIGINT"))
            .addField(DynamicField.of("amount", "DECIMAL"))
            .addField(DynamicField.of("quantity", "INTEGER"))
            .addField(DynamicField.of("version", "BIGINT")).build();

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
    }

    private static ConditionGroup where(long id) {
        return ConditionGroup.and().where("id", "=", id).build();
    }

    @Test
    void ordinaryAndOptimisticUpdatesPublishTheirCompletedParameterList() {
        FormDataSqlRenderer renderer = renderer();
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("amount", ValueExpression.field("amount").add(ValueExpression.value(2)));
        values.put("quantity", null);
        for (SqlRequest request : List.of(renderer.update(FORM, values, where(3)),
                renderer.update(FORM, values, where(3), OptimisticLockOptions.increment("version", 7L)))) {
            assertTrue(OwnedBindableValues.isPublished(request.parameters()));
            assertEquals(2, request.parameters().get(0));
            assertEquals(Integer.class, assertInstanceOf(SqlNullParameter.class,
                    request.parameters().get(1)).javaType());
            assertEquals(3L, request.parameters().get(2));
            if (request.parameters().size() == 4) assertEquals(7L, request.parameters().get(3));
            assertThrows(UnsupportedOperationException.class, () -> request.parameters().set(0, 9));
        }
    }

    @Test
    void publishedUpdateParametersStillSnapshotMutableCodecValues() {
        DynamicForm form = DynamicForm.builder("files", "files")
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("payload", "VARBINARY"))
                .addField(DynamicField.of("version", "BIGINT")).build();
        FormDataSqlRenderer renderer = renderer();
        byte[] bytes = {1, 2};
        SqlRequest ordinary = renderer.update(form, Map.of("payload", bytes), where(3));
        SqlRequest locked = renderer.update(form, Map.of("payload", bytes), where(3),
                OptimisticLockOptions.increment("version", 7L));
        bytes[0] = 9;
        for (SqlRequest request : List.of(ordinary, locked)) {
            byte[] exported = assertInstanceOf(byte[].class, request.parameters().getFirst());
            assertArrayEquals(new byte[]{1, 2}, exported);
            exported[0] = 8;
            assertArrayEquals(new byte[]{1, 2}, (byte[]) request.parameters().getFirst());
        }
    }

    @Test
    void expressionBindsPrecedeWhereAndLockAndCacheNeverRetainsValues() {
        FormDataSqlRenderer renderer = renderer();
        OptimisticLockOptions lock = OptimisticLockOptions.increment("version", 7L);
        for (int amount : List.of(3, 5)) {
            SqlRequest request = renderer.update(FORM, Map.of("amount",
                    ValueExpression.field("amount").coalesce(ValueExpression.value(amount))
                            .multiply(ValueExpression.field("quantity"))), where(1), lock);
            assertTrue(request.sql().contains("\"amount\" = (coalesce(\"amount\", ?) * \"quantity\")"), request.sql());
            assertEquals(List.of(amount, 1L, 7L), request.parameters());
        }
        SqlRequest changed = renderer.update(FORM,
                Map.of("amount", ValueExpression.field("amount").add(ValueExpression.value(2))), where(2));
        assertTrue(changed.sql().contains("\"amount\" = (\"amount\" + ?)"), changed.sql());
        assertEquals(List.of(2, 2L), changed.parameters());
    }

    @Test
    void batchLayoutSupportsZeroAndMultipleExpressionParameters() {
        FormDataSqlRenderer renderer = renderer();
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("quantity", ValueExpression.field("quantity"));
        values.put("amount", ValueExpression.value(2).add(ValueExpression.value(3)));
        SqlRequest request = renderer.update(FORM, values, where(9));
        BatchUpdatePlan plan = renderer.batchUpdatePlan(FORM, values, null, request);
        assertEquals(List.of(Integer.class, Integer.class, Long.class), plan.parameterTypes());
        assertArrayEquals(new Object[]{2, 3, 9L}, plan.parameters(request, 0, null));
    }

    @Test
    void singleAssignmentRetainsCanonicalColumnAndTypedNull() {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("QUANTITY", null);
        SqlRequest request = renderer().update(FORM, values, where(3));
        assertEquals("update \"sales\" set \"quantity\" = ? where \"id\" = ?", request.sql());
        com.flying.orm.rdb.internal.binding.SqlNullParameter parameter = assertInstanceOf(
                com.flying.orm.rdb.internal.binding.SqlNullParameter.class, request.parameters().getFirst());
        assertEquals(Integer.class, parameter.javaType());
        assertEquals(3L, request.parameters().get(1));
    }

    @Test
    void multipleAssignmentsStillRejectNormalizedDuplicates() {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("quantity", 1);
        values.put("QUANTITY", ValueExpression.value(2));
        FormDataSqlRenderer renderer = renderer();
        assertEquals("duplicate normalized dynamic update field", assertThrows(IllegalArgumentException.class,
                () -> renderer.update(FORM, values, where(3))).getMessage());
        assertEquals("duplicate normalized dynamic update field", assertThrows(IllegalArgumentException.class,
                () -> renderer.update(FORM, values, where(3), OptimisticLockOptions.increment("version", 1L)))
                .getMessage());
    }

    @Test
    void multipleAssignmentsKeepDeclarationAndBindingOrder() {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("VERSION", 9L);
        values.put("QUANTITY", 4);
        SqlRequest request = renderer().update(FORM, values, where(3));
        assertEquals("update \"sales\" set \"version\" = ?, \"quantity\" = ? where \"id\" = ?", request.sql());
        assertEquals(List.of(9L, 4, 3L), request.parameters());
    }

    @Test
    void insertMustNotSerializeAnExecutableExpressionAsAValue() {
        SqlRequest request = renderer().insert(FORM, Map.of("amount", ValueExpression.value(1)));
        assertEquals(List.of(1), request.parameters());
        assertFalse(request.sql().contains("ValueExpression"));
    }

    private static FormOperationPlanner planner() {
        FormDataSqlRenderer renderer = renderer();
        return new FormOperationPlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited());
    }

    @Test
    void copyingFromAnUnreadableFieldMustNotBypassScope() {
        WriteSpec spec = WriteSpec.update(FORM, Map.of("amount", ValueExpression.field("quantity")), where(1))
                .withScope(DataScope.none().withFields(FieldScope.readable("id", "amount")));
        assertThrows(ScopeAccessException.class, () -> planner().update(spec));
    }

    @Test
    void governedExpressionRequiresFullSourceValueNotOnlyWritableTarget() {
        WriteSpec spec = WriteSpec.update(FORM, Map.of("amount", ValueExpression.field("quantity")), where(1));
        FieldUsePolicy denied = FieldUsePolicy.builder().allow("amount", FieldUse.UPDATE)
                .allow("id", FieldUse.FILTER).build();
        assertThrows(ScopeAccessException.class,
                () -> planner().write(spec, FormOperationPlanner.WriteAction.UPDATE, true, denied, QueryShapeLimits.defaults()));
        FieldUsePolicy allowed = FieldUsePolicy.builder().allow("amount", FieldUse.UPDATE)
                .allow("id", FieldUse.FILTER).allow("quantity", FieldUse.FULL_VALUE).build();
        assertDoesNotThrow(() -> planner().write(spec, FormOperationPlanner.WriteAction.UPDATE, true, allowed, QueryShapeLimits.defaults()));
    }

    @Test
    void encryptedSourcesAndTargetsAreRejectedBeforePhysicalPlanning() {
        DynamicForm form = DynamicForm.builder("protected", "protected")
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .addField(DynamicField.of("plain", "VARCHAR"))
                .encrypted("secret", com.flying.orm.core.protection.EncryptedFieldDefinition.builder().build()).build();
        for (Map<String, Object> values : List.<Map<String, Object>>of(
                Map.of("plain", ValueExpression.field("secret")),
                Map.of("secret", ValueExpression.field("plain")))) {
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> planner().update(WriteSpec.update(form, values, where(1))));
            assertTrue(failure.getMessage().contains("encrypted field"));
        }
    }
}
