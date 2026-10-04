package com.flying.orm.rdb.architecture;

import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUseRequirements;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.FormAggregateReadSupport;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.condition.ConditionNodes;
import com.flying.orm.rdb.internal.mapping.EntityEnumValueCodec;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.internal.template.SqlStatements;
import com.flying.orm.rdb.jdbc.JdbcBatchWriter;
import com.flying.orm.rdb.jdbc.JdbcSqlExecutor;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.protection.ProtectedRelationalSchemaProjector;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.function.LongConsumer;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FormInternalOwnerSurfaceConvergenceTest {

    @Test
    void protectedWriteBoundaryIsConsistentAcrossContractsAndImplementations() throws Exception {
        for (Class<?> type : List.of(SyncSqlExecutor.class, JdbcSqlExecutor.class,
                ReactiveSqlExecutor.class, R2dbcSqlExecutor.class)) {
            assertTrue(type.getDeclaredMethod("protectedWrite", ProtectedWriteWork.class,
                    SqlExecutionOptions.class).isAnnotationPresent(InternalApi.class), type.getName());
            assertFalse(type.getMethod("rowsUpdated", SqlRequest.class,
                    SqlExecutionOptions.class).isAnnotationPresent(InternalApi.class), type.getName());
        }
    }

    @Test
    void internalBatchCallbacksDoNotBecomePublicApiThroughTheirImplementation() throws Exception {
        for (Class<?> type : List.of(SyncBatchExecutor.class, JdbcBatchWriter.class)) {
            for (String name : List.of("writeBatch", "writeProtectedBatch")) {
                assertTrue(type.getDeclaredMethod(name, BatchWriteRequest.class,
                        LongConsumer.class).isAnnotationPresent(InternalApi.class), type.getName() + "." + name);
                assertFalse(type.getMethod(name, BatchWriteRequest.class)
                        .isAnnotationPresent(InternalApi.class), type.getName() + "." + name);
            }
        }
    }

    @Test
    void internalPackagesDoNotRepeatTheirBoundaryAsAnnotations() {
        for (Class<?> type : List.of(ConditionNodes.class, EntityEnumValueCodec.class,
                EntityValues.class, SqlStatements.class)) {
            assertTrue(type.getPackageName().startsWith("com.flying.orm.rdb.internal."));
            assertFalse(type.isAnnotationPresent(InternalApi.class), type.getName());
            assertNoRepeatedInternalMarkers(type);
        }
    }

    @Test
    void typeWideInternalBoundariesDoNotRepeatMarkersOnTheirMembers() {
        for (Class<?> type : List.of(FormAggregateReadSupport.class, ProtectedBatchRows.class,
                ProtectedWriteWork.class, ProtectedFieldRuntime.class, ProtectedRelationalSchemaProjector.class)) {
            assertTrue(type.isAnnotationPresent(InternalApi.class), type.getName());
            assertNoRepeatedInternalMarkers(type);
        }
    }

    private static void assertNoRepeatedInternalMarkers(Class<?> type) {
        for (Method method : type.getDeclaredMethods()) {
            assertFalse(method.isAnnotationPresent(InternalApi.class), method.toGenericString());
        }
        for (Class<?> nested : type.getDeclaredClasses()) {
            assertFalse(nested.isAnnotationPresent(InternalApi.class), nested.getName());
            assertNoRepeatedInternalMarkers(nested);
        }
    }

    @Test
    void documentedEntityMappingRegistryIsPublicOnBothClients() throws Exception {
        for (Class<?> client : List.of(ReactiveFormClient.class, SyncFormClient.class)) {
            Method method = client.getMethod("entityModels");
            assertEquals(EntityModelRegistry.class, method.getReturnType());
            assertFalse(method.isAnnotationPresent(InternalApi.class), client.getSimpleName());
        }
    }

    @Test
    void conditionRendererStaysInsideTheFormPackage() throws Exception {
        Method method = FormDataSqlRenderer.class.getDeclaredMethod("conditionRenderer");
        assertFalse(Modifier.isPublic(method.getModifiers()));
    }

    @Test
    void formOwnersDoNotExposeSingleUseTopLevelHelpers() {
        List<String> obsoleteTypes = List.of(
                "com.flying.orm.rdb.form.KeysetPredicateRenderer",
                "com.flying.orm.rdb.form.KeysetCursorVisibilityGuard",
                "com.flying.orm.rdb.form.JoinFieldUseGuard",
                "com.flying.orm.rdb.form.JoinSourceSqlRenderer");

        assertAll(obsoleteTypes.stream()
                .map(typeName -> () -> assertThrows(
                        ClassNotFoundException.class,
                        () -> Class.forName(typeName, false,
                                FormInternalOwnerSurfaceConvergenceTest.class.getClassLoader()))));
    }

    @Test
    void fieldUseGuardKeepsItsImplementationInsideTheFormPackage() throws Exception {
        Class<?> guard = Class.forName("com.flying.orm.rdb.form.FieldUseGuard");
        Method aggregate = guard.getDeclaredMethod(
                "approveAggregate",
                String.class,
                FieldUseRequirements.class,
                FieldScope.class,
                SqlRequest.class,
                FieldUsePolicy.class,
                QueryShapeLimits.class,
                int.class,
                int.class,
                int.class,
                int.class,
                int.class);
        Method term = guard.getDeclaredMethod(
                "approveTermExtension",
                FormDataSqlRenderer.class,
                TermCondition.class,
                FieldUse.class);

        assertAll(
                () -> assertFalse(Modifier.isPublic(guard.getModifiers())),
                () -> assertFalse(Modifier.isPublic(aggregate.getModifiers())),
                () -> assertFalse(Modifier.isPublic(term.getModifiers())));
    }

    @Test
    void aggregateReadSupportOwnsTheEquivalentCrossPackageApprovalBridge() throws Exception {
        Method bridge = FormAggregateReadSupport.class.getMethod(
                "approveAggregate",
                String.class,
                FieldUseRequirements.class,
                FieldScope.class,
                SqlRequest.class,
                FieldUsePolicy.class,
                QueryShapeLimits.class,
                int.class,
                int.class,
                int.class,
                int.class,
                int.class);

        assertAll(
                () -> assertEquals(FieldUseSnapshot.class, bridge.getReturnType()),
                () -> assertTrue(Modifier.isPublic(bridge.getModifiers())),
                () -> assertTrue(bridge.getDeclaringClass().isAnnotationPresent(InternalApi.class)));
    }
}
