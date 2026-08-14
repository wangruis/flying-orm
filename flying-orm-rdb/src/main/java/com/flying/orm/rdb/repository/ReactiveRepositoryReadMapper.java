package com.flying.orm.rdb.repository;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.mapping.EntityMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 收口实体 Repository 的查询规格和结果后置映射。
 *
 * <p>逻辑删除条件仍使用已有的 RepositoryLogicDeletes 组合，Scope 和执行保护只写入 QuerySpec，
 * 最终查询、字段解码和安全校验仍由 ReactiveFormClient 完成。分页只替换 rows，total、页码和页大小原样保留。</p>
 *
 * @param <T> 实体类型
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class ReactiveRepositoryReadMapper<T> {

    private final ReactiveFormClient client;
    private final DynamicForm form;
    private final Class<T> entityType;
    private final EntityMetadata<T> metadata;
    private final ReactiveRepositoryLifecycleSupport<T> lifecycle;

    ReactiveRepositoryReadMapper(ReactiveFormClient client,
                                 DynamicForm form,
                                 Class<T> entityType,
                                 EntityMetadata<T> metadata,
                                 ReactiveRepositoryLifecycleSupport<T> lifecycle) {
        this.client = Objects.requireNonNull(client, "reactive form client must not be null");
        this.form = Objects.requireNonNull(form, "repository form must not be null");
        this.entityType = Objects.requireNonNull(entityType, "repository entity type must not be null");
        this.metadata = Objects.requireNonNull(metadata, "repository entity metadata must not be null");
        this.lifecycle = Objects.requireNonNull(lifecycle, "repository lifecycle support must not be null");
    }

    Flux<T> select(ConditionGroup where, DataScope scope, SqlExecutionOptions options) {
        return lifecycle.postLoad(client.select(querySpec(where, scope, options), entityType));
    }

    Mono<PageResult<T>> page(ConditionGroup where,
                             PageQuery page,
                             DataScope scope,
                             SqlExecutionOptions options) {
        return client.page(querySpec(where, scope, options), page, entityType)
                .flatMap(lifecycle::postLoad);
    }

    private QuerySpec querySpec(ConditionGroup where,
                                DataScope scope,
                                SqlExecutionOptions options) {
        QuerySpec spec = RepositoryQueryDefaults.apply(
                QuerySpec.of(form, RepositoryLogicDeletes.activeWhere(metadata, form, where)), metadata);
        if (scope != null) {
            spec = spec.withScope(scope);
        }
        return options == null ? spec : spec.withExecutionOptions(options);
    }
}
