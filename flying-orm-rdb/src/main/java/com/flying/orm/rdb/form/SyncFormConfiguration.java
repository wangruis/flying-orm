package com.flying.orm.rdb.form;

import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.mapping.EntityModelRegistry;

import java.util.Objects;

/** 原生同步表单运行时共享的不可变安全配置。 */
record SyncFormConfiguration(FormDataSqlRenderer renderer,
                             StructuredConditionResolver resolver,
                             DataScope dataScope,
                             SqlExecutionOptions executionOptions,
                             BatchWriteOptions batchOptions,
                             EntityModelRegistry entityModels) {

    SyncFormConfiguration {
        renderer = Objects.requireNonNull(renderer, "form data sql renderer must not be null");
        resolver = Objects.requireNonNull(resolver, "structured condition resolver must not be null");
        dataScope = Objects.requireNonNull(dataScope, "default data scope must not be null");
        executionOptions = Objects.requireNonNull(executionOptions, "default execution options must not be null");
        batchOptions = Objects.requireNonNull(batchOptions, "default batch options must not be null");
        entityModels = Objects.requireNonNull(entityModels, "entity model registry must not be null");
    }

    SyncFormConfiguration withResolver(StructuredConditionResolver value) {
        return new SyncFormConfiguration(renderer, value, dataScope, executionOptions, batchOptions, entityModels);
    }

    SyncFormConfiguration withExecutionOptions(SqlExecutionOptions value) {
        return new SyncFormConfiguration(renderer, resolver, dataScope, value, batchOptions, entityModels);
    }

    SyncFormConfiguration withDataScope(DataScope value) {
        return new SyncFormConfiguration(renderer, resolver, value, executionOptions, batchOptions, entityModels);
    }

    SyncFormConfiguration withBatchOptions(BatchWriteOptions value) {
        return new SyncFormConfiguration(renderer, resolver, dataScope, executionOptions, value, entityModels);
    }

    SyncFormConfiguration withEntityModels(EntityModelRegistry value) {
        return new SyncFormConfiguration(renderer, resolver, dataScope, executionOptions, batchOptions, value);
    }
}
