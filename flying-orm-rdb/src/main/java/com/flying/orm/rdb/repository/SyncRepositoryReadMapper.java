package com.flying.orm.rdb.repository;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.lock.LockingReadSpec;
import com.flying.orm.rdb.lock.ReadLock;

import java.util.List;
import java.util.Objects;

/**
 * 同步实体查询的规格组装和结果回调。
 *
 * <p>这里不碰连接、SQL 渲染或字段解码。它只补充实体定义的逻辑删除条件，并把 Scope 与执行保护写进
 * {@link QuerySpec}，随后让同步表单客户端使用当前运行时执行。</p>
 */
final class SyncRepositoryReadMapper<T> {

    private final SyncFormClient client;
    private final DynamicForm form;
    private final Class<T> entityType;
    private final EntityMetadata<T> metadata;
    private final SyncRepositoryLifecycleSupport<T> lifecycle;

    SyncRepositoryReadMapper(SyncFormClient client,
                             DynamicForm form,
                             Class<T> entityType,
                             EntityMetadata<T> metadata,
                             SyncRepositoryLifecycleSupport<T> lifecycle) {
        this.client = Objects.requireNonNull(client, "sync form client must not be null");
        this.form = Objects.requireNonNull(form, "repository form must not be null");
        this.entityType = Objects.requireNonNull(entityType, "repository entity type must not be null");
        this.metadata = Objects.requireNonNull(metadata, "repository entity metadata must not be null");
        this.lifecycle = Objects.requireNonNull(lifecycle, "repository lifecycle support must not be null");
    }

    List<T> select(ConditionGroup where, DataScope scope, SqlExecutionOptions options) {
        return lifecycle.postLoad(client.select(querySpec(where, scope, options), entityType));
    }

    List<T> lockingRead(ConditionGroup where, ReadLock lock) {
        QuerySpec query = querySpec(where, null, null);
        return lifecycle.postLoad(client.lockingRead(
                LockingReadSpec.of(query, Objects.requireNonNull(
                        lock, "repository read lock must not be null")), entityType));
    }

    PageResult<T> page(ConditionGroup where, PageQuery page, DataScope scope, SqlExecutionOptions options) {
        return lifecycle.postLoad(client.page(querySpec(where, scope, options), page, entityType));
    }

    private QuerySpec querySpec(ConditionGroup where, DataScope scope, SqlExecutionOptions options) {
        QuerySpec spec = RepositoryQueryDefaults.apply(
                QuerySpec.of(form, RepositoryLogicDeletes.activeWhere(metadata, form, where)), metadata);
        if (scope != null) {
            spec = spec.withScope(scope);
        }
        return options == null ? spec : spec.withExecutionOptions(options);
    }
}
