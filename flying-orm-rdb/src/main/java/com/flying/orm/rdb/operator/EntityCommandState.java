package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.mapping.EntityMetadata;

import java.util.Objects;

/**
 * 一个实体 Lambda 命令共用的可变状态。
 *
 * <p>这里故意只保存执行方式无关的信息：实体表单、字段解析器、条件树和显式数据范围。它不持有
 * ReactiveFormClient、SyncFormClient、连接或订阅对象，所以同一份 SQL 计划可以被两种执行方式使用。
 * 命令对象不是线程安全的，和普通 fluent builder 一样应当一次调用链只由一个线程组装。</p>
 *
 * @param <T> 实体类型
 */
final class EntityCommandState<T> {

    private final EntityMetadata<T> metadata;
    private final DynamicForm form;
    private final EntityWhereBuilder<T> where;
    private DataScope scope = DataScope.none();

    EntityCommandState(EntityMetadata<T> metadata, DynamicForm form, SqlRenderer renderer) {
        this.metadata = Objects.requireNonNull(metadata, "entity metadata must not be null");
        this.form = Objects.requireNonNull(form, "dynamic form must not be null");
        this.where = new EntityWhereBuilder<>(metadata, renderer);
    }

    EntityMetadata<T> metadata() {
        return metadata;
    }

    DynamicForm form() {
        return form;
    }

    EntityWhereBuilder<T> where() {
        return where;
    }

    DataScope scope() {
        return scope;
    }

    void scope(DataScope scope) {
        this.scope = this.scope.and(Objects.requireNonNull(scope, "data scope must not be null"));
    }
}
