package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.plan.StructuralPlanCaches;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class SqlCacheBypassPlanningTest {
    @Test
    void disabledSqlCacheNeverBuildsTheFormFingerprint() throws Exception {
        Field fingerprint = DynamicForm.class.getDeclaredField("structureFingerprint");
        fingerprint.setAccessible(true);
        for (boolean cacheConditions : List.of(false, true)) {
            StructuralPlanCaches caches = caches(false, cacheConditions);
            FormDataSqlRenderer renderer = renderer(caches);
            List<Function<DynamicForm, SqlRequest>> operations = List.of(
                    form -> renderer.insert(form, Map.of("name", "first")),
                    form -> renderer.select(form, where(1)),
                    form -> renderer.update(form, Map.of("name", "first"), where(1)),
                    form -> renderer.update(form, Map.of("name", "first"), where(1),
                            OptimisticLockOptions.assign("version", 1L, 2L)),
                    form -> renderer.delete(form, where(1)),
                    form -> renderer.delete(form, where(1), OptimisticLockOptions.increment("version", 1L)));
            for (Function<DynamicForm, SqlRequest> operation : operations) {
                DynamicForm form = form();
                assertNull(fingerprint.get(form));
                SqlRequest request = operation.apply(form);
                assertFalse(request.sql().isBlank());
                assertTrue(request.statement().transportSql("postgresql").isPresent());
                assertNull(fingerprint.get(form), "disabled SQL cache must not build its structure key");
            }
            assertEquals(0, caches.sqlSnapshot().estimatedSize());
        }
    }

    @Test
    void enabledCacheStillReusesAndInvalidatesStatementsWithoutRetainingValues() {
        StructuralPlanCaches caches = caches(true, true);
        FormDataSqlRenderer renderer = renderer(caches);
        DynamicForm form = form();
        SqlRequest first = renderer.update(form, Map.of("name", "first"), where(1));
        SqlRequest second = renderer.update(form, Map.of("name", "second"), where(2));
        assertSame(first.statement(), second.statement());
        assertEquals(List.of("first", 1L), first.parameters());
        assertEquals(List.of("second", 2L), second.parameters());
        caches.invalidate("items");
        SqlRequest refreshed = renderer.update(form, Map.of("name", "third"), where(3));
        assertNotSame(first.statement(), refreshed.statement());
        assertEquals(List.of("third", 3L), refreshed.parameters());
    }

    @Test
    void cacheSwitchesPreserveExpressionAndOptimisticAssignmentBindings() {
        DynamicForm form = form();
        SqlRequest expected = renderer(caches(true, true)).update(form,
                Map.of("name", ValueExpression.field("name").coalesce(ValueExpression.value("fallback"))),
                where(3), OptimisticLockOptions.assign("version", 4L, 5L));
        assertEquals(List.of("fallback", 5L, 3L, 4L), expected.parameters());
        for (boolean sql : List.of(false, true)) {
            for (boolean conditions : List.of(false, true)) {
                FormDataSqlRenderer renderer = renderer(caches(sql, conditions));
                SqlRequest actual = renderer.update(form,
                        Map.of("name", ValueExpression.field("name").coalesce(ValueExpression.value("fallback"))),
                        where(3), OptimisticLockOptions.assign("version", 4L, 5L));
                assertEquals(expected.sql(), actual.sql());
                assertEquals(expected.parameters(), actual.parameters());
                assertEquals(expected.statement().transportSql("postgresql"),
                        actual.statement().transportSql("postgresql"));
                SqlRequest unrestricted = renderer.update(form, Map.of("name", "all rows"),
                        ConditionGroup.and().build());
                assertEquals("update \"items\" set \"name\" = ?", unrestricted.sql());
                assertEquals(List.of("all rows"), unrestricted.parameters());
            }
        }
    }

    private static StructuralPlanCaches caches(boolean sql, boolean conditions) {
        return StructuralPlanCaches.create(OrmCachePolicy.builder()
                .sqlPlans(sql ? CacheRegionPolicy.sqlPlanDefaults() : CacheRegionPolicy.disabled())
                .conditionPlans(conditions ? CacheRegionPolicy.conditionPlanDefaults() : CacheRegionPolicy.disabled())
                .build());
    }

    private static FormDataSqlRenderer renderer(StructuralPlanCaches caches) {
        return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql(),
                caches, CacheRegionPolicy.sqlPlanDefaults(), ProtectedFieldRuntime.withoutKeys(), List.of());
    }

    private static ConditionGroup where(long id) {
        return ConditionGroup.and().where("id", "=", id).build();
    }

    private static DynamicForm form() {
        return DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR"))
                .addField(DynamicField.of("version", "BIGINT")).build();
    }
}
