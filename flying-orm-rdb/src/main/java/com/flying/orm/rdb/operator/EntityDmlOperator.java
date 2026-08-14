package com.flying.orm.rdb.operator;

import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;

import java.util.Objects;

/**
 * 绑定单一实体类型的响应式 Lambda DML 入口。
 *
 * <p>入口只保留可并发复用的实体模型和渲染器。每次 query/update/delete 都会新建独立命令，
 * 所以同一个入口可安全地放在 Repository 或 Service 中长期复用，不会串条件、排序或写入值。</p>
 *
 * @param <T> 实体类型
 * @author wangr
 * @version v2.0.0
 */
public final class EntityDmlOperator<T> {

    private final ReactiveFormClient client;
    private final SqlRenderer renderer;
    private final EntityDmlModel<T> model;

    private EntityDmlOperator(ReactiveFormClient client, SqlRenderer renderer, Class<T> type) {
        this.client = Objects.requireNonNull(client, "form client must not be null");
        this.renderer = Objects.requireNonNull(renderer, "sql renderer must not be null");
        this.model = new EntityDmlModel<>(this.client.entityModels()
                                                  .metadata(Objects.requireNonNull(type, "entity type must not be null")));
    }

    /** 创建绑定实体映射的响应式 DML 入口。 */
    public static <T> EntityDmlOperator<T> create(ReactiveFormClient client,
                                                   SqlRenderer renderer,
                                                   Class<T> type) {
        return new EntityDmlOperator<>(client, renderer, type);
    }

    /** @return 当前实体的新查询命令 */
    public EntityDmlQueryOperator<T> query() {
        return new EntityDmlQueryOperator<>(client, new EntityQueryCommand<>(model.newState(renderer)));
    }

    /** @return 当前实体的新更新命令 */
    public EntityDmlUpdateOperator<T> update() {
        return new EntityDmlUpdateOperator<>(client, new EntityUpdateCommand<>(model.newState(renderer)));
    }

    /** @return 当前实体的新删除命令；存在逻辑删除定义时默认执行逻辑删除 */
    public EntityDmlDeleteOperator<T> delete() {
        return new EntityDmlDeleteOperator<>(client, new EntityDeleteCommand<>(model.newState(renderer)));
    }
}
