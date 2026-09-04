package com.flying.orm.rdb.operator;

import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.util.Objects;

/**
 * 动态数据的同步链式入口。
 *
 * <p>它只保存 {@link SyncFormClient}、{@link SyncSqlExecutor} 和共享渲染配置，最终直接执行 JDBC。
 * 每个 query、update 或 delete 调用都会创建自己的轻量命令对象。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public final class SyncDmlOperator {

    private final SyncFormClient formClient;
    private final SyncSqlExecutor executor;
    private final SqlRenderer renderer;
    private final DataScope defaultDataScope;

    /** 原生 JDBC 构造器，统一由同步 DatabaseOperator 装配。 */
    SyncDmlOperator(SyncFormClient formClient,
                    SyncSqlExecutor executor,
                    SqlRenderer renderer,
                    DataScope defaultDataScope) {
        this.formClient = Objects.requireNonNull(formClient, "sync form client must not be null");
        this.executor = Objects.requireNonNull(executor, "sync sql executor must not be null");
        this.renderer = Objects.requireNonNull(renderer, "sql renderer must not be null");
        this.defaultDataScope = Objects.requireNonNull(defaultDataScope, "default data scope must not be null");
    }

    /** 创建单次同步查询构建器。 */
    public SyncQueryOperator query() {
        return new SyncQueryOperator(formClient, executor, renderer, defaultDataScope);
    }

    /** 创建以 DynamicForm 为根源的原生 JDBC 轻量多表查询。 */
    public SyncJoinQueryOperator joinQuery(DynamicForm rootForm) {
        return new SyncJoinQueryOperator(formClient, renderer, rootForm);
    }

    /** 创建以实体类型为根源的原生 JDBC Lambda 轻量多表查询。 */
    public <T> SyncEntityJoinQueryOperator<T> joinQuery(Class<T> rootType) {
        return new SyncEntityJoinQueryOperator<>(formClient, renderer, rootType);
    }

    /** 创建单次同步更新构建器。 */
    public SyncDmlUpdateOperator update(String table) {
        return new SyncDmlUpdateOperator(formClient, renderer, table);
    }

    /** 创建单次同步删除构建器。 */
    public SyncDmlDeleteOperator delete(String table) {
        return new SyncDmlDeleteOperator(formClient, renderer, table);
    }
}
