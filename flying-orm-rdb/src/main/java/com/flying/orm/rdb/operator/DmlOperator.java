package com.flying.orm.rdb.operator;

import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;

import java.util.Objects;

/**
 * 动态数据的响应式链式入口。
 *
 * <p>这里不直接拼接值或执行 SQL，只创建查询、更新、删除构建器。每个构建器只供一次业务操作使用，
 * 最终仍进入统一的条件 AST、SqlRenderer、ReactiveFormClient 和执行保护链路。</p>
 *
 * @author wangr
 * @date 2026-07-27
 * @version v1.0
 */
public final class DmlOperator {

    private final ReactiveFormClient formClient;

    private final ReactiveSqlExecutor executor;

    private final SqlRenderer renderer;

    private final DataScope defaultDataScope;

    DmlOperator(ReactiveFormClient formClient,
                ReactiveSqlExecutor executor,
                SqlRenderer renderer,
                DataScope defaultDataScope) {
        this.formClient = Objects.requireNonNull(formClient, "form client must not be null");
        this.executor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        this.renderer = Objects.requireNonNull(renderer, "sql renderer must not be null");
        this.defaultDataScope = Objects.requireNonNull(defaultDataScope, "default data scope must not be null");
    }

    /**
     * 创建一条新的动态查询命令。
     *
     * @return 只供本次查询使用的构建器
     */
    public QueryOperator query() {
        return new QueryOperator(executor, renderer, defaultDataScope);
    }

    /** 创建以 DynamicForm 为根源的轻量多表查询。 */
    public JoinQueryOperator joinQuery(DynamicForm rootForm) {
        return new JoinQueryOperator(formClient, renderer, rootForm);
    }

    /** 创建以实体类型为根源的 Lambda 轻量多表查询。 */
    public <T> EntityJoinQueryOperator<T> joinQuery(Class<T> rootType) {
        return new EntityJoinQueryOperator<>(formClient, renderer, rootType);
    }

    /**
     * 创建一条更新命令。表名立即做标识符校验，字段和值在后续 set/execute 阶段处理。
     *
     * @param table 目标物理表
     * @return 更新构建器
     */
    public DmlUpdateOperator update(String table) {
        return new DmlUpdateOperator(formClient, renderer, table);
    }

    /**
     * 创建一条删除命令。是否逻辑删除由后续 {@code logicDelete(...)} 与 {@code physical()} 决定。
     *
     * @param table 目标物理表
     * @return 删除构建器
     */
    public DmlDeleteOperator delete(String table) {
        return new DmlDeleteOperator(formClient, renderer, table);
    }

    /**
     * 绑定实体类型并创建不需要表名、字段名字符串的 Lambda DML 入口。
     *
     * <p>实体表单由约定元数据自动生成并缓存。该入口适合显式操作其他实体；上层业务封装可以进一步
     * 隐藏 {@code Class<T>}，直接暴露 {@code createUpdate()}、{@code createDelete()} 等短入口。</p>
     *
     * @param type 目标实体类型
     * @param <T> 实体类型
     * @return 绑定实体元数据的 Lambda DML 入口
     */
    public <T> EntityDmlOperator<T> entity(Class<T> type) {
        return formClient.entity(type);
    }

    ReactiveFormClient formClient() {
        return formClient;
    }
}
