package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;

import java.util.Objects;

/**
 * 各类表单操作共用的运行时依赖。
 *
 * <p>这里不放查询、写入或批量编排，只负责把不可变配置快照拆成可复用的 Scope 与结果处理协作者。
 * 具体操作类只读取这些稳定引用，所以一个客户端可以安全地被多个响应式订阅并发使用。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
class ReactiveFormOperationSupport {

    final ReactiveSqlExecutor executor;
    final FormDataSqlRenderer renderer;
    final SqlExecutionOptions defaultExecutionOptions;
    final BatchWriteOptions defaultBatchWriteOptions;
    final EntityModelRegistry entityModels;
    final FormScopeSupport scopes;
    final FormOperationPlanner planner;
    final FormResultDecoder results;
    final ProtectedContainsResultSupport containsResults;
    final FieldUsePolicy fieldUsePolicy;
    final QueryShapeLimits queryShapeLimits;
    final boolean governed;

    final FormConfiguration configuration;

    ReactiveFormOperationSupport(ReactiveSqlExecutor executor, FormConfiguration configuration) {
        this.executor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        this.configuration = Objects.requireNonNull(configuration, "form configuration must not be null");
        this.renderer = configuration.renderer();
        this.defaultExecutionOptions = configuration.executionOptions();
        this.defaultBatchWriteOptions = configuration.batchOptions();
        this.entityModels = configuration.entityModels();
        this.fieldUsePolicy = configuration.fieldUsePolicy();
        this.queryShapeLimits = configuration.queryShapeLimits();
        this.governed = configuration.governed();
        this.scopes = new FormScopeSupport(renderer, configuration.resolver(), configuration.dataScope());
        this.planner = new FormOperationPlanner(renderer, scopes, defaultExecutionOptions);
        this.results = new FormResultDecoder(renderer, entityModels);
        this.containsResults = new ProtectedContainsResultSupport(renderer);
    }

    /**
     * 让各职责协作者复用同一套不可变运行时对象。
     *
     * <p>一个客户端只需要创建一次 Scope 合并器和结果映射器。后续查询、分页、写入、删除与批量协作者
     * 只复制稳定引用，不重复创建缓存入口，也不会在执行 SQL 时产生额外的配置查找。</p>
     */
    ReactiveFormOperationSupport(ReactiveFormOperationSupport source) {
        ReactiveFormOperationSupport shared = Objects.requireNonNull(source,
                                                                      "shared form runtime must not be null");
        this.configuration = shared.configuration;
        this.executor = shared.executor;
        this.renderer = shared.renderer;
        this.defaultExecutionOptions = shared.defaultExecutionOptions;
        this.defaultBatchWriteOptions = shared.defaultBatchWriteOptions;
        this.entityModels = shared.entityModels;
        this.scopes = shared.scopes;
        this.planner = shared.planner;
        this.results = shared.results;
        this.containsResults = shared.containsResults;
        this.fieldUsePolicy = shared.fieldUsePolicy;
        this.queryShapeLimits = shared.queryShapeLimits;
        this.governed = shared.governed;
    }

}
