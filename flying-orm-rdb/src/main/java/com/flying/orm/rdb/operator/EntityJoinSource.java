package com.flying.orm.rdb.operator;

import com.flying.orm.core.join.JoinCondition;
import com.flying.orm.core.join.JoinFieldRef;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.lambda.EntityProperty;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.internal.mapping.EntityPropertyResolver;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.EntityModelRegistry;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * ON 回调中的实体来源：Getter 解析字段，值条件沿用实体枚举及字段映射。
 *
 * @param <T> 实体类型
 * @author wangr
 * @version v4.1.2
 */
public final class EntityJoinSource<T> {
    private final JoinSource source;
    private final EntityMetadata<T> metadata;
    private final EntityModelRegistry models;
    private final SqlRenderer renderer;

    EntityJoinSource(JoinSource source, EntityMetadata<T> metadata, EntityModelRegistry models, SqlRenderer renderer) {
        this.source = source;
        this.metadata = metadata;
        this.models = models;
        this.renderer = renderer;
    }

    /** 引用本来源的实体字段，可与另一个来源字段比较。 */
    public JoinFieldRef field(EntityProperty<T, ?> property) {
        return source.field(EntityPropertyResolver.column(metadata, property));
    }

    /** 构建本来源内的 ON 值条件，不追加到查询的 WHERE。 */
    public JoinCondition where(Consumer<EntityCondition<T>> consumer) {
        Objects.requireNonNull(consumer, "join entity condition consumer must not be null");
        EntityCondition<T> condition = new EntityCondition<>(metadata, renderer,
                metadata.hasEnumConditionValues() ? models.entityValues(metadata.type()) : null);
        consumer.accept(condition);
        return JoinCondition.where(source, condition.build());
    }
}
