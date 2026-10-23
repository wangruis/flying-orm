package com.flying.orm.rdb.repository;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.EntityQueryDefaults;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 固定 Repository 已完成的内部表面积收敛，避免同一查询装配或布尔决策再次长成独立实现。 */
class RepositorySurfaceConvergenceTest {

    @Test
    void syncAndReactiveBatchPreparationShareOneOperationModel() {
        Class<?> kind = assertDoesNotThrow(() -> Class.forName(
                "com.flying.orm.rdb.repository.RepositoryBatchKind"));
        assertTrue(kind.isEnum());
        for (Class<?> coordinator : List.of(SyncRepositoryBatchCoordinator.class,
                ReactiveRepositoryBatchCoordinator.class)) {
            assertTrue(java.util.Arrays.stream(coordinator.getDeclaredMethods())
                    .noneMatch(method -> List.of("insert", "upsert", "update", "insertIfAbsent",
                            "upsertSingle", "insertIfAbsentSingle").contains(method.getName())),
                    "operation-specific public entries must not be repeated as internal forwarding chains");
            assertTrue(java.util.Arrays.stream(coordinator.getDeclaredMethods())
                    .anyMatch(method -> List.of(method.getParameterTypes()).contains(kind)),
                    "both execution modes must consume the shared batch operation model");
        }
    }

    @Test
    void reactiveBatchEntryHasOneCoordinator() {
        org.junit.jupiter.api.Assertions.assertEquals(1L, java.util.Arrays.stream(
                ReactiveFormRepository.class.getDeclaredFields())
                .filter(field -> field.getType() == ReactiveRepositoryBatchCoordinator.class).count(),
                "repository batch specifications and execution must not retain separate forwarding owners");
    }

    @Test
    void syncBatchCoordinatorHasNoPrivateForwardingSupportOwner() {
        org.junit.jupiter.api.Assertions.assertFalse(java.util.Arrays.stream(
                SyncRepositoryBatchCoordinator.class.getDeclaredFields())
                .anyMatch(field -> field.getType().getSimpleName().equals("SyncRepositoryBatchSupport")),
                "batch operation selection and entity row preparation belong to one coordinator");
    }

    @Test
    void querySpecificationAssemblyHasOneOwner() {
        Method query = assertDoesNotThrow(() -> EntityQueryDefaults.class.getDeclaredMethod(
                "query",
                DynamicForm.class,
                EntityMetadata.class,
                ConditionGroup.class,
                FieldScope.class));

        assertTrue(Modifier.isStatic(query.getModifiers()));
        assertThrows(NoSuchMethodException.class, () -> EntityQueryDefaults.class.getDeclaredMethod(
                "query", DynamicForm.class, EntityMetadata.class, ConditionGroup.class,
                DataScope.class, SqlExecutionOptions.class, FieldScope.class));
    }

    @Test
    void repositoryReadsHaveNoMappinglessForwardingLayer() {
        for (String name : List.of("SyncRepositoryReadMapper", "ReactiveRepositoryReadMapper")) {
            assertThrows(ClassNotFoundException.class,
                    () -> Class.forName("com.flying.orm.rdb.repository." + name));
        }
    }

    @Test
    void batchTrackingDecisionHasNoStandalonePlanType() {
        assertThrows(ClassNotFoundException.class,
                     () -> Class.forName("com.flying.orm.rdb.repository.RepositoryBatchLifecyclePlan"));
    }

}
