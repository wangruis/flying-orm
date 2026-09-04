package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.internal.cache.SchemaCacheInvalidationCoordinator;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;

/** 响应式 Schema 执行的缓存失效生命周期。 */
final class ReactiveSchemaInvalidationScope {

    private final Consumer<String> metadataInvalidator;
    private final List<String> tables;
    private volatile boolean executionStarted;
    private volatile boolean transactionCompletionRegistered;
    private volatile Throwable executionFailure;

    ReactiveSchemaInvalidationScope(Consumer<String> metadataInvalidator, List<String> tables) {
        this.metadataInvalidator = metadataInvalidator;
        this.tables = tables;
    }

    void executionStarted() {
        executionStarted = true;
    }

    void executionFailed(Throwable failure) {
        executionFailure = failure;
    }

    boolean registerTransactionCompletion(R2dbcTransactionContext transaction) {
        boolean registered = transaction.completion().register(
                ignored -> Mono.fromRunnable(this::invalidate));
        if (registered) {
            transactionCompletionRegistered = true;
        }
        return registered;
    }

    boolean externalTransactionRegistered() {
        return transactionCompletionRegistered;
    }

    void publisherTerminated() {
        if (!transactionCompletionRegistered) {
            invalidatePreservingPrimary();
        }
    }

    private void invalidatePreservingPrimary() {
        try {
            invalidate();
        } catch (RuntimeException invalidationFailure) {
            Throwable primary = executionFailure;
            if (primary == null) {
                throw invalidationFailure;
            }
            if (primary != invalidationFailure) {
                primary.addSuppressed(invalidationFailure);
            }
        }
    }

    private void invalidate() {
        if (!executionStarted) {
            return;
        }
        SchemaCacheInvalidationCoordinator.invalidateTables(metadataInvalidator, tables);
    }
}
