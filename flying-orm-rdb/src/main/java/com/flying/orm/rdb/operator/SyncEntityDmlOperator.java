package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.internal.InternalApi;

import java.util.Objects;

/**
 * 类型化实体 DML 的同步入口。
 *
 * <p>它直接保存 {@link SyncFormClient}，执行时走原生同步 SQL 运行时，不会创建响应式 Entity Operator、
 * 订阅 Publisher 或调用 block。字段、条件与 SQL 计划继续和响应式入口共用同一组命令对象。</p>
 *
 * @param <T> 实体类型
 * @author wangr
 * @version v2.0.0
 */
public final class SyncEntityDmlOperator<T> {

    private final SyncFormClient client;
    private final SqlRenderer renderer;
    private final EntityDmlModel<T> model;
    private SyncEntityDmlOperator(SyncFormClient client, SqlRenderer renderer, Class<T> type) {
        this(client, renderer, null, type);
    }

    private SyncEntityDmlOperator(SyncFormClient client,
                                  SqlRenderer renderer,
                                  DynamicForm form,
                                  Class<T> type) {
        this.client = Objects.requireNonNull(client, "sync form client must not be null");
        this.renderer = Objects.requireNonNull(renderer, "sql renderer must not be null");
        var metadata = client.entityModels().metadata(Objects.requireNonNull(type, "entity type must not be null"));
        this.model = form == null ? new EntityDmlModel<>(metadata) : new EntityDmlModel<>(metadata, form);
    }

    /**
     * 直接从同步表单客户端创建实体 DML 入口。
     *
     * <p>这个入口不要求调用方知道 JDBC 细节。表单客户端决定实际同步执行器、事务参与方式和执行保护，
     * renderer 只负责把已经验证的实体字段与条件渲染成统一 SQL。</p>
     */
    public static <T> SyncEntityDmlOperator<T> create(SyncFormClient client, SqlRenderer renderer, Class<T> type) {
        return new SyncEntityDmlOperator<>(client, renderer, type);
    }

    /** Repository 内部使用显式表单时，实体字段映射不应偷偷换回注解生成的表单。 */
    @InternalApi
    public static <T> SyncEntityDmlOperator<T> create(SyncFormClient client,
                                                       SqlRenderer renderer,
                                                       DynamicForm form,
                                                       Class<T> type) {
        return new SyncEntityDmlOperator<>(client, renderer,
                                           Objects.requireNonNull(form, "entity dynamic form must not be null"), type);
    }

    /** @return 当前实体的新同步查询命令 */
    public SyncEntityDmlQueryOperator<T> query() {
        return new SyncEntityDmlQueryOperator<>(client, new EntityQueryCommand<>(model.newState(renderer)));
    }

    /** @return 当前实体的新同步更新命令 */
    public SyncEntityDmlUpdateOperator<T> update() {
        return new SyncEntityDmlUpdateOperator<>(client, new EntityUpdateCommand<>(model.newState(renderer)));
    }

    /** @return 当前实体的新同步删除命令 */
    public SyncEntityDmlDeleteOperator<T> delete() {
        return new SyncEntityDmlDeleteOperator<>(client, new EntityDeleteCommand<>(model.newState(renderer)));
    }
}
