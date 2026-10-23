package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.Version;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.schema.SchemaMigrationObserver;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 固定 Operator 内部状态的唯一所有者，避免已经收回的单调用方胶水类型再次出现。 */
class OperatorSurfaceConvergenceTest {
    @Test
    void conditionsAndQueriesHaveCanonicalTerminalsWithoutDuplicateAliases() {
        for (Class<?> type : List.of(EntityDmlQueryOperator.class, SyncEntityDmlQueryOperator.class)) {
            assertFalse(java.util.Arrays.stream(type.getDeclaredMethods())
                    .anyMatch(method -> method.getName().equals("execute")));
        }
        for (Class<?> type : List.of(WhereDsl.class, EntityCondition.class)) {
            assertFalse(java.util.Arrays.stream(type.getDeclaredMethods())
                    .anyMatch(method -> List.of("term", "termIfPresent", "isIfPresent").contains(method.getName())));
        }
        assertFalse(java.util.Arrays.stream(WhereDsl.class.getDeclaredConstructors())
                .anyMatch(constructor -> constructor.getParameterCount() == 0));
    }

    @Test
    void entityOperatorsDoNotExposeUnusedMutableCommands() {
        for (Class<?> type : List.of(EntityDmlQueryOperator.class, EntityDmlUpdateOperator.class,
                EntityDmlDeleteOperator.class)) {
            assertThrows(NoSuchMethodException.class, () -> type.getDeclaredMethod("command"));
        }
    }

    @Test
    void entityUpdateAndDeleteShareVersionRulesWithoutSharingRequestState() {
        EntityDmlModel<Versioned> model = model(Versioned.class);
        PlanningUpdate<Versioned> update = new PlanningUpdate<>(model);
        PlanningDelete<Versioned> delete = new PlanningDelete<>(model);
        update.optimisticLock(2L);
        delete.optimisticLock(3L);
        assertEquals(OptimisticLockOptions.increment("revision_no", 2L), update.spec().lock().orElseThrow());
        assertEquals(OptimisticLockOptions.increment("revision_no", 3L), delete.spec().lock().orElseThrow());
        String nullMessage = "expected entity version must not be null: " + Versioned.class.getName() + ".revision";
        assertEquals(nullMessage, assertThrows(MappingException.class, () -> update.optimisticLock(null)).getMessage());
        assertEquals(nullMessage, assertThrows(MappingException.class, () -> delete.optimisticLock(null)).getMessage());
        assertEquals(2L, update.spec().lock().orElseThrow().expectedValue());
        assertEquals(3L, delete.spec().lock().orElseThrow().expectedValue());
    }

    @Test
    void invalidEntityVersionsKeepTheirMappingFailures() {
        String missing = "entity has no @Version field: " + Unversioned.class.getName();
        assertEquals(missing, assertThrows(MappingException.class,
                () -> new PlanningUpdate<>(model(Unversioned.class)).optimisticLock(1L)).getMessage());
        assertEquals(missing, assertThrows(MappingException.class,
                () -> new PlanningDelete<>(model(Unversioned.class)).optimisticLock(1L)).getMessage());
        String nonNumeric = "entity version field cannot be incremented automatically: "
                + TextVersion.class.getName() + ".revision (VARCHAR)";
        assertEquals(nonNumeric, assertThrows(MappingException.class,
                () -> new PlanningUpdate<>(model(TextVersion.class)).optimisticLock("v1")).getMessage());
        assertEquals(nonNumeric, assertThrows(MappingException.class,
                () -> new PlanningDelete<>(model(TextVersion.class)).optimisticLock("v1")).getMessage());
    }

    @Test
    void standaloneEntityVersionPlanningHelperIsAbsent() {
        assertThrows(ClassNotFoundException.class,
                () -> Class.forName("com.flying.orm.rdb.operator.EntityOptimisticLocks"));
    }

    private static final class PlanningUpdate<T> extends EntityUpdateCommand<T, PlanningUpdate<T>> {
        PlanningUpdate(EntityDmlModel<T> model) {
            super(model, SqlRenderer.builder().addDefaultTerms().build(), EntityModelRegistry.create(CacheRegionPolicy.disabled()));
        }
        @Override protected PlanningUpdate<T> self() { return this; }
    }

    private static final class PlanningDelete<T> extends EntityDeleteCommand<T, PlanningDelete<T>> {
        PlanningDelete(EntityDmlModel<T> model) { super(model, SqlRenderer.builder().addDefaultTerms().build()); }
        @Override protected PlanningDelete<T> self() { return this; }
    }

    private static <T> EntityDmlModel<T> model(Class<T> type) {
        return new EntityDmlModel<>(EntityModelRegistry.create(CacheRegionPolicy.disabled()), null, type);
    }

    record Versioned(@Version @TableField("revision_no") Long revision) { }
    record TextVersion(@Version String revision) { }
    record Unversioned(String name) { }

    @Test
    void syncOperatorConfigurationIsPublicWhileAssemblyStaysInternal() throws Exception {
        assertFalse(SyncDatabaseOperator.class.getMethod("withDefaultDataScope", DataScope.class)
                .isAnnotationPresent(InternalApi.class));
        assertFalse(SyncDatabaseOperator.class.getMethod("withDefaultBatchWriteOptions", BatchWriteOptions.class)
                .isAnnotationPresent(InternalApi.class));
        assertFalse(SyncDatabaseOperator.class.getMethod("withSchemaExecutionOptions", SqlExecutionOptions.class)
                .isAnnotationPresent(InternalApi.class));
        assertFalse(SyncDatabaseOperator.class.getMethod("withSchemaObserver", SchemaMigrationObserver.class)
                .isAnnotationPresent(InternalApi.class));
        assertTrue(SyncDatabaseOperator.class.getMethod("create", SyncDatabaseOperator.NativeComponents.class)
                .isAnnotationPresent(InternalApi.class));
        assertTrue(SyncDatabaseOperator.NativeComponents.class.isAnnotationPresent(InternalApi.class));
    }

    @Test
    void dynamicWriteCommandOwnsItsTransientFormClosure() {
        PlanningDynamicWrite.Update command = new PlanningDynamicWrite.Update(
                SqlRenderer.builder().addDefaultTerms().build(), "accounts");
        command.set("name", "Ada");
        command.where(where -> where.is("id", 7L).is("tenant_id", "tenant-a"));
        command.optimisticLock(OptimisticLockOptions.increment("version", 3L));
        command.logicDelete("deleted", 0, 1);

        WriteSpec spec = command.spec();
        DynamicForm form = spec.form();

        assertEquals("accounts", form.table());
        assertEquals(List.of("name", "id", "tenant_id", "version", "deleted"),
                     form.fields().stream().map(field -> field.name()).toList());
        assertEquals(List.of("name"), spec.values().keySet().stream().toList());
        assertEquals("version", spec.lock().orElseThrow().field());
        assertTrue(form.logicDelete().isPresent());
    }

    @Test
    void transientWriteFieldsUseTheSameIdentityAcrossAssignmentsAndPredicates() {
        PlanningDynamicWrite.Update command = new PlanningDynamicWrite.Update(
                SqlRenderer.builder().addDefaultTerms().build(), "accounts");
        command.setExpression("name", ValueExpression.field("NAME"));
        command.where(where -> where.is("NAME", "Ada").is("version", 3L).is("deleted", 0));
        command.optimisticLock(OptimisticLockOptions.increment("VERSION", 3L));
        command.logicDelete("DELETED", 0, 1);

        WriteSpec spec = assertDoesNotThrow(command::spec);
        assertEquals(List.of("name", "version", "deleted"),
                spec.form().fields().stream().map(field -> field.name()).toList());
        assertEquals("name", spec.form().field("NAME").name());
        assertEquals("VERSION", spec.lock().orElseThrow().field());
    }

    @Test
    void deletePredicatesCanReferToOneFieldWithDifferentSpelling() {
        PlanningDynamicWrite.Delete command = new PlanningDynamicWrite.Delete(
                SqlRenderer.builder().addDefaultTerms().build(), "accounts");
        command.where(where -> where.is("id", 7L).is("ID", 7L).is("deleted", 0));
        command.logicDelete("DELETED", 0, 1);
        WriteSpec spec = assertDoesNotThrow(command::spec);
        assertEquals(List.of("id", "deleted"),
                spec.form().fields().stream().map(field -> field.name()).toList());
    }

    @Test
    void singleCallerDmlFormBuilderTypeIsAbsent() {
        assertThrows(ClassNotFoundException.class,
                     () -> Class.forName("com.flying.orm.rdb.operator.DmlFormBuilder"));
    }
}
