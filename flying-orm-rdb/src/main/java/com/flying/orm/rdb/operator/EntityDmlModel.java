package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.internal.mapping.EntityValues;

import java.util.Objects;

/**
 * 一个实体类型在 Lambda DML 中真正不变的部分。
 *
 * <p>实体元数据和动态表单都来自同一次映射解析。把它们放在这个小对象中，响应式和同步入口就能创建
 * 完全一样的命令状态，而不必彼此包装或重复解析反射元数据。每次 query/update/delete 仍会创建自己的
 * 可变命令状态，因此这个模型本身可以被多个线程安全地复用。</p>
 *
 * @param <T> 实体类型
 */
final class EntityDmlModel<T> {

    private final EntityMetadata<T> metadata;
    private final DynamicForm form;
    private final EntityValues<T> entityValues;

    EntityDmlModel(EntityModelRegistry models, DynamicForm form, Class<T> type) {
        this.metadata = models.metadata(Objects.requireNonNull(type, "entity type must not be null"));
        this.form = form == null ? metadata.toDynamicForm() : form;
        this.entityValues = metadata.hasEnumConditionValues() ? models.entityValues(type) : null;
    }

    EntityMetadata<T> metadata() { return metadata; }
    DynamicForm form() { return form; }
    EntityValues<T> entityValues() { return entityValues; }
}
