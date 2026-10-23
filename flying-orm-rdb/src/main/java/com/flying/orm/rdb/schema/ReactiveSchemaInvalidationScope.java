package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.internal.cache.SchemaCacheInvalidationCoordinator;

import java.util.List;
import java.util.function.Consumer;

/** 响应式 Schema 执行的缓存失效生命周期。 */
final class ReactiveSchemaInvalidationScope<T> {

    private final Consumer<T> metadataInvalidator;
    private final List<T> tables;
    private volatile boolean executionStarted;
    private volatile Throwable executionFailure;

    ReactiveSchemaInvalidationScope(Consumer<T> metadataInvalidator, List<T> tables) {
        this.metadataInvalidator = metadataInvalidator;
        this.tables = tables;
    }

    void executionStarted() {
        executionStarted = true;
    }

    void executionFailed(Throwable failure) {
        executionFailure = failure;
    }

    void publisherTerminated() {
        if (executionStarted) {
            SchemaCacheInvalidationCoordinator.invalidateTables(metadataInvalidator, tables, executionFailure);
        }
    }
}
