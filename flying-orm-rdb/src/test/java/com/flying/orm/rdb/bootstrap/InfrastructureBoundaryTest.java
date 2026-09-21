package com.flying.orm.rdb.bootstrap;

import org.junit.jupiter.api.Test;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.observation.SqlExecutionLogSelection;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;

import java.util.Arrays;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertFalse;

class InfrastructureBoundaryTest {

    @Test
    void receiptRecoveryHasNoPublicConfigurationModelsOrExecutorMethods() {
        for (Class<?> type : new Class<?>[]{BatchWriteOptions.class, BatchExecutionEvidence.class,
                BatchExecutionObservation.class, ReactiveSqlExecutor.class, R2dbcSqlExecutor.class,
                ProtectedBatchRows.class, ProtectedBatchRows.RowView.class,
                SqlExecutionLogSelection.class}) {
            assertFalse(Arrays.stream(type.getDeclaredMethods()).anyMatch(method ->
                    isRecoveryName(method.getName())), type.getName() + " retains recovery methods");
            assertFalse(Arrays.stream(type.getDeclaredFields()).anyMatch(field ->
                    isRecoveryName(field.getName())), type.getName() + " retains recovery fields");
            assertFalse(Arrays.stream(type.getDeclaredClasses()).anyMatch(nested ->
                    isRecoveryName(nested.getSimpleName())), type.getName() + " retains recovery models");
        }
        for (String retired : new String[]{"BatchResolution", "BatchReceiptIntegrityException",
                "BatchReceiptMismatchException"}) {
            assertFalse(java.nio.file.Files.exists(java.nio.file.Path.of(
                    "src/main/java/com/flying/orm/rdb/batch/" + retired + ".java")),
                    retired + " must be deleted");
        }
    }

    @Test
    void fieldProtectionHasNoReceiptIdentityApi() {
        for (Class<?> type : new Class<?>[]{ProtectedFieldRuntime.class, ProtectedFieldRuntime.WriteOperation.class}) {
            assertFalse(Arrays.stream(type.getDeclaredMethods()).anyMatch(method ->
                    isRecoveryName(method.getName())), type.getName() + " retains receipt identity methods");
        }
    }

    private static boolean isRecoveryName(String name) {
        String normalized = name.toLowerCase(Locale.ROOT);
        return normalized.contains("recovery") || normalized.contains("receipt")
                || normalized.equals("resolveunknown");
    }

    @Test
    void metadataFactoryAcceptsExecutorsInsteadOfDataSources() {
        assertFalse(Arrays.stream(JdbcFormMetadataReaders.class.getDeclaredMethods())
                .flatMap(method -> Arrays.stream(method.getParameterTypes()))
                .anyMatch(type -> type.getName().equals("javax.sql.DataSource")));
    }

    @Test
    void environmentDoesNotStorePhysicalTopology() {
        assertFalse(Arrays.stream(FlyingOrmEnvironment.class.getRecordComponents())
                .anyMatch(component -> component.getName().startsWith("physical")));
        assertFalse(Arrays.stream(FlyingOrmEnvironment.class.getDeclaredMethods())
                .anyMatch(method -> method.getName().toLowerCase(Locale.ROOT).contains("physical")));
    }

    @Test
    void builderDoesNotRetainTopologyValidationStubs() {
        assertFalse(Arrays.stream(FlyingOrmClientBuilder.class.getDeclaredMethods())
                .anyMatch(method -> method.getName().startsWith("validate")
                        && method.getName().contains("DataSource")));
    }
}
