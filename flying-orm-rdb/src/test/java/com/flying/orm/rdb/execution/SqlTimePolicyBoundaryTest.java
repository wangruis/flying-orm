package com.flying.orm.rdb.execution;

import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.internal.sync.SyncBlockingGuard;
import com.flying.orm.rdb.observation.SqlFailureCategory;
import com.flying.orm.rdb.schema.SchemaDialect;
import com.flying.orm.rdb.schema.SchemaMigrationExecutionOptions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.sql.SQLTimeoutException;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SqlTimePolicyBoundaryTest {

    @Test
    void batchOptionsHaveNoExecutionOrConfirmationTimePolicy() {
        assertNoTimePolicy(BatchWriteOptions.class);
    }

    @Test
    void sqlOptionsExposeCapacityButNoTimePolicy() {
        assertNoTimePolicy(SqlExecutionOptions.class);
    }

    @Test
    void schemaOptionsExposeApprovalButNoTimePolicy() {
        assertNoTimePolicy(SchemaMigrationExecutionOptions.class);
    }

    @Test
    void syncClientHasNoTimeoutCompatibilitySurface() {
        assertNoTimePolicy(SyncFormClient.class);
    }

    @Test
    void schemaDialectHasNoSessionTimeoutConfiguration() {
        assertNoTimePolicy(SchemaDialect.Builder.class);
    }

    @Test
    void syncLifecycleHasNoTimeoutOverloads() {
        assertNoTimePolicy(SyncBlockingGuard.class);
    }

    @Test
    void externalTimeoutErrorsRemainObservableWithoutOrmTimePolicy() {
        assertAll(
                () -> assertEquals(SqlFailureCategory.TIMEOUT,
                        SqlFailureCategory.classify(new TimeoutException("upstream deadline"))),
                () -> assertEquals(SqlFailureCategory.TIMEOUT,
                        SqlFailureCategory.classify(new IllegalStateException(
                                new SQLTimeoutException("driver timeout")))));
    }

    private static void assertNoTimePolicy(Class<?> type) {
        assertAll(type.getSimpleName(),
                () -> assertFalse(Arrays.stream(type.getDeclaredMethods()).anyMatch(method ->
                        method.getName().toLowerCase(Locale.ROOT).contains("timeout")),
                        "time-policy methods must be deleted, not retained as rejecting stubs"),
                () -> assertFalse(Arrays.stream(type.getDeclaredFields()).anyMatch(field ->
                        field.getType() == Duration.class
                                || field.getName().toLowerCase(Locale.ROOT).contains("timeout")),
                        "time-policy fields must be deleted"),
                () -> assertFalse(Arrays.stream(type.getDeclaredConstructors())
                        .flatMap(constructor -> Arrays.stream(constructor.getParameterTypes()))
                        .anyMatch(Duration.class::equals),
                        "constructors must not accept time-policy configuration"),
                () -> assertFalse(Arrays.stream(type.getDeclaredMethods())
                        .flatMap(method -> Arrays.stream(method.getParameterTypes()))
                        .anyMatch(Duration.class::equals),
                        "methods must not accept time-policy configuration"));
    }
}
