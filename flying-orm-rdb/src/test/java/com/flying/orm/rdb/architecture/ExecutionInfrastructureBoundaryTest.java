package com.flying.orm.rdb.architecture;

import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Checks the actual compiled production tree, not just the public facade names. */
class ExecutionInfrastructureBoundaryTest {
    private static final List<String> RETIRED = List.of(
            "transaction.JdbcTransactionCompletion", "transaction.JdbcTransactionContext",
            "transaction.JdbcTransactionParticipant", "transaction.R2dbcTransactionCompletion",
            "transaction.R2dbcTransactionContext", "transaction.R2dbcTransactionParticipant",
            "transaction.R2dbcTransactionParticipationException", "transaction.TransactionOutcome",
            "jdbc.JdbcConnectionProvider", "jdbc.JdbcExternalTransactionConnectionView",
            "jdbc.JdbcIndependentBatchExecutor", "jdbc.JdbcBatchEvidenceExecutor",
            "reactive.R2dbcBatchConnectionLifecycle", "reactive.R2dbcIndependentBatchFlow",
            "reactive.R2dbcIndependentBatchWriter", "reactive.R2dbcAtomicBatchWriter",
            "reactive.R2dbcConnectionLeaseCleanup",
            "batch.BatchWriteResult", "batch.BatchChunkResult", "batch.BatchCommitFact",
            "observation.SqlTransactionSource", "observation.BatchSummaryMetrics",
            "execution.SqlExecutionTimeoutException", "execution.QueryRoutingIntent",
            "migration.ReactiveDataMigration", "migration.DataMigrationException",
            "migration.DataMigrationPlan", "migration.DataMigrationResult",
            "migration.DataMigrationStatus", "migration.DataMigrationStep",
            "migration.DataMigrationStepResult");

    @Test
    void retiredInfrastructureTypesAreAbsentFromTheCompiledArtifact() {
        for (String name : RETIRED) {
            assertThrows(ClassNotFoundException.class,
                    () -> Class.forName("com.flying.orm.rdb." + name, false, getClass().getClassLoader()), name);
        }
    }

    @Test
    void everyProductionTypeResolvesWithoutInfrastructureOwnerContracts() throws Exception {
        Path root = Path.of(FlyingOrmClients.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        Path packages = root.resolve("com/flying/orm/rdb");
        assertTrue(Files.isDirectory(packages), "the module test runs against current compiled production classes");
        try (var files = Files.walk(packages)) {
            for (Path file : files.filter(path -> path.toString().endsWith(".class")).toList()) {
                String name = root.relativize(file).toString().replace('\\', '.').replace('/', '.');
                Class<?> type = Class.forName(name.substring(0, name.length() - 6), false,
                        getClass().getClassLoader());
                for (Field field : type.getDeclaredFields()) check(field.toGenericString());
                for (Constructor<?> constructor : type.getDeclaredConstructors()) check(constructor.toGenericString());
                for (Method method : type.getDeclaredMethods()) check(method.toGenericString());
                for (Class<?> dependency : type.getInterfaces()) check(dependency.getName());
            }
        }
    }

    private static void check(String signature) {
        assertFalse(signature.contains("javax.sql.DataSource")
                || signature.contains("io.r2dbc.spi.ConnectionFactory")
                || signature.contains("com.flying.orm.rdb.transaction."), signature);
    }
}
