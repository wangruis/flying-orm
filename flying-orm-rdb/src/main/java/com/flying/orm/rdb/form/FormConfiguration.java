package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.mapping.EntityModelRegistry;

import java.util.Objects;

/** JDBC 与 R2DBC 表单客户端共用的不可变配置。 */
record FormConfiguration(FormDataSqlRenderer renderer,
                         StructuredConditionResolver resolver,
                         DataScope dataScope,
                         SqlExecutionOptions executionOptions,
                         BatchWriteOptions batchOptions,
                         EntityModelRegistry entityModels,
                         FieldUsePolicy fieldUsePolicy,
                         QueryShapeLimits queryShapeLimits) {

    FormConfiguration {
        renderer = Objects.requireNonNull(renderer, "form data sql renderer must not be null");
        resolver = Objects.requireNonNull(resolver, "structured condition resolver must not be null");
        dataScope = Objects.requireNonNull(dataScope, "default data scope must not be null");
        executionOptions = Objects.requireNonNull(executionOptions, "default execution options must not be null");
        batchOptions = Objects.requireNonNull(batchOptions, "default batch options must not be null");
        entityModels = Objects.requireNonNull(entityModels, "entity model registry must not be null");
        fieldUsePolicy = Objects.requireNonNull(fieldUsePolicy, "field use policy must not be null");
        queryShapeLimits = Objects.requireNonNull(queryShapeLimits, "query shape limits must not be null");
    }

    FormConfiguration withResolver(StructuredConditionResolver value) {
        return new FormConfiguration(renderer, value, dataScope, executionOptions, batchOptions, entityModels,
                                     fieldUsePolicy, queryShapeLimits);
    }

    FormConfiguration withExecutionOptions(SqlExecutionOptions value) {
        return new FormConfiguration(renderer, resolver, dataScope, value, batchOptions, entityModels,
                                     fieldUsePolicy, queryShapeLimits);
    }

    FormConfiguration withDataScope(DataScope value) {
        return new FormConfiguration(renderer, resolver, value, executionOptions, batchOptions, entityModels,
                                     fieldUsePolicy, queryShapeLimits);
    }

    FormConfiguration withBatchOptions(BatchWriteOptions value) {
        return new FormConfiguration(renderer, resolver, dataScope, executionOptions, value, entityModels,
                                     fieldUsePolicy, queryShapeLimits);
    }

    FormConfiguration withEntityModels(EntityModelRegistry value) {
        return new FormConfiguration(renderer, resolver, dataScope, executionOptions, batchOptions, value,
                                     fieldUsePolicy, queryShapeLimits);
    }

    FormConfiguration withFieldUsePolicy(FieldUsePolicy value) {
        return new FormConfiguration(renderer, resolver, dataScope, executionOptions, batchOptions, entityModels,
                                     value, queryShapeLimits);
    }

    FormConfiguration withQueryShapeLimits(QueryShapeLimits value) {
        return new FormConfiguration(renderer, resolver, dataScope, executionOptions, batchOptions, entityModels,
                                     fieldUsePolicy, value);
    }
}
