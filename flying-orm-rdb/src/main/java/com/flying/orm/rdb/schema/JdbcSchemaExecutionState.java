package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.execution.SqlExecutionStepResult;
import com.flying.orm.rdb.internal.cache.SchemaCacheInvalidationCoordinator;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static com.flying.orm.rdb.schema.JdbcSchemaExecutionSupport.suppress;

/** JDBC Schema 执行期间跨阶段共享的状态，并统一缓存失效终态。 */
final class JdbcSchemaExecutionState {

    final List<SqlExecutionStepResult> steps = new ArrayList<>();
    int failedStepIndex = -1;
    boolean started;

    void invalidateAfterExecution(List<String> tables,
                                  Consumer<String> invalidator,
                                  Throwable primaryFailure) {
        if (!started) {
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

    private static void invalidateTables(Consumer<String> invalidator, List<String> tables) {
        SchemaCacheInvalidationCoordinator.invalidateTables(invalidator, tables);
    }
}
