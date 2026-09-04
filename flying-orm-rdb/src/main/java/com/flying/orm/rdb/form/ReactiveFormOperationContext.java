package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;

import java.util.Objects;

/**
 * 动态表单操作链路共享的一份不可变配置快照。
 *
 * <p>客户端的 {@code with...} 方法只创建新的快照，已经拿到旧客户端的订阅不会看到后续配置变化。
 * 这样 Scope、执行保护、批量策略和实体映射缓存始终来自同一份装配结果。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
record ReactiveFormOperationContext(ReactiveSqlExecutor executor,
                                    FormDataSqlRenderer renderer,
                                    StructuredConditionResolver structuredConditionResolver,
                                    DataScope defaultDataScope,
                                    SqlExecutionOptions defaultExecutionOptions,
                                    BatchWriteOptions defaultBatchWriteOptions,
                                    EntityModelRegistry entityModels,
                                    FieldUsePolicy fieldUsePolicy,
                                    QueryShapeLimits queryShapeLimits) {

    ReactiveFormOperationContext {
        executor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        renderer = Objects.requireNonNull(renderer, "form data sql renderer must not be null");
        structuredConditionResolver = Objects.requireNonNull(structuredConditionResolver,
                                                              "structured condition resolver must not be null");
        defaultDataScope = Objects.requireNonNull(defaultDataScope, "default data scope must not be null");
        defaultExecutionOptions = Objects.requireNonNull(defaultExecutionOptions,
                                                         "default sql execution options must not be null");
        defaultBatchWriteOptions = Objects.requireNonNull(defaultBatchWriteOptions,
                                                          "default batch write options must not be null");
        entityModels = Objects.requireNonNull(entityModels, "entity model registry must not be null");
        fieldUsePolicy = Objects.requireNonNull(fieldUsePolicy, "field use policy must not be null");
        queryShapeLimits = Objects.requireNonNull(queryShapeLimits, "query shape limits must not be null");
    }

    ReactiveFormOperationContext withResolver(StructuredConditionResolver resolver) {
        return new ReactiveFormOperationContext(executor, renderer, resolver, defaultDataScope,
                                                defaultExecutionOptions, defaultBatchWriteOptions, entityModels,
                                                fieldUsePolicy, queryShapeLimits);
    }

    ReactiveFormOperationContext withExecutionOptions(SqlExecutionOptions options) {
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        return new ReactiveFormOperationContext(executor.withDefaultExecutionOptions(safeOptions), renderer,
                                                structuredConditionResolver, defaultDataScope, safeOptions,
                                                defaultBatchWriteOptions, entityModels,
                                                fieldUsePolicy, queryShapeLimits);
    }

    ReactiveFormOperationContext withDataScope(DataScope scope) {
        return new ReactiveFormOperationContext(executor, renderer, structuredConditionResolver, scope,
                                                defaultExecutionOptions, defaultBatchWriteOptions, entityModels,
                                                fieldUsePolicy, queryShapeLimits);
    }

    ReactiveFormOperationContext withBatchWriteOptions(BatchWriteOptions options) {
        return new ReactiveFormOperationContext(executor, renderer, structuredConditionResolver, defaultDataScope,
                                                defaultExecutionOptions, options, entityModels,
                                                fieldUsePolicy, queryShapeLimits);
    }

    ReactiveFormOperationContext withEntityModels(EntityModelRegistry registry) {
        return new ReactiveFormOperationContext(executor, renderer, structuredConditionResolver, defaultDataScope,
                                                defaultExecutionOptions, defaultBatchWriteOptions, registry,
                                                fieldUsePolicy, queryShapeLimits);
    }

    ReactiveFormOperationContext withFieldUsePolicy(FieldUsePolicy policy) {
        return new ReactiveFormOperationContext(executor, renderer, structuredConditionResolver, defaultDataScope,
                                                defaultExecutionOptions, defaultBatchWriteOptions, entityModels,
                                                policy, queryShapeLimits);
    }

    ReactiveFormOperationContext withQueryShapeLimits(QueryShapeLimits limits) {
        return new ReactiveFormOperationContext(executor, renderer, structuredConditionResolver, defaultDataScope,
                                                defaultExecutionOptions, defaultBatchWriteOptions, entityModels,
                                                fieldUsePolicy, limits);
    }
}
