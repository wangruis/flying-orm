package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.execution.SqlExecutionPhase;
import com.flying.orm.rdb.execution.SqlExecutionStepResult;
import com.flying.orm.rdb.internal.cache.SchemaCacheInvalidationCoordinator;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static com.flying.orm.rdb.schema.JdbcSchemaExecutionSupport.suppress;

/** JDBC Schema 执行期间跨阶段共享的状态，并统一缓存失效终态。 */
final class JdbcSchemaExecutionState {

    final List<SqlExecutionStepResult> steps = new ArrayList<>();
    SqlExecutionPhase phase;
    int failedStepIndex = -1;
    volatile boolean started;
    volatile boolean transactionCompletionRegistered;

    void invalidateAfterExecution(List<String> tables,
                                  Consumer<String> invalidator,
                                  Throwable primaryFailure) {
        if (!started || transactionCompletionRegistered) {
            return;
        }
        try {
            invalidateTables(invalidator, tables);
        } catch (RuntimeException invalidationFailure) {
            if (primaryFailure == null) {
                throw invalidationFailure;
            }
            suppress(primaryFailure, invalidationFailure);
        }
    }

    void invalidateAtTransactionCompletion(List<String> tables, Consumer<String> invalidator) {
        if (started) {
            invalidateTables(invalidator, tables);
        }
    }

    private static void invalidateTables(Consumer<String> invalidator, List<String> tables) {
        SchemaCacheInvalidationCoordinator.invalidateTables(invalidator, tables);
    }
}
