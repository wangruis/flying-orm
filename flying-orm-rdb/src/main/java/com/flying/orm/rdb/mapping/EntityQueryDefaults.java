package com.flying.orm.rdb.mapping;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.internal.InternalApi;

import java.util.Comparator;
import java.util.List;

/**
 * 实体读取的内部默认规则；投影供所有实体入口共享，默认排序仍只用于 Repository。
 *
 * @author wangr
 * @version v4.0.0
 */
@InternalApi
public final class EntityQueryDefaults {

    private EntityQueryDefaults() {
    }

    /**
     * 组装实体 Repository 共用的读取规格。
     *
     * <p>这里只装配默认投影和默认排序；传入条件已由 Repository 完成实体值转换。
     * 默认字段范围用于裁窄投影，行级 Scope、逻辑删除与执行选项仍由表单执行链处理。</p>
     */
    public static QuerySpec query(DynamicForm form,
                                  EntityMetadata<?> metadata,
                                  ConditionGroup where,
                                  FieldScope defaultFields) {
        QuerySpec projected = applyProjection(QuerySpec.of(form, where), metadata, defaultFields);
        List<PageSort> sorts = metadata.fields().stream()
                .filter(EntityFieldMetadata::ordered)
                .sorted(Comparator.comparingInt(EntityFieldMetadata::orderPriority))
                .map(field -> field.orderAscending()
                        ? PageSort.asc(field.columnName()) : PageSort.desc(field.columnName()))
                .toList();
        return sorts.isEmpty() ? projected : projected.withSorts(sorts);
    }

    /**
     * 仅用于未显式 select 的实体读取。默认列不是调用方主动申请的字段，因此受限 Scope 可以继续把它裁窄；
     * 显式 select 仍走原有字段治理并保持严格拒绝。
     */
    public static QuerySpec applyProjection(QuerySpec spec, EntityMetadata<?> metadata, FieldScope defaultFields) {
        List<String> readable = metadata.defaultProjection();
        if (readable == null) {
            return spec;
        }
        FieldScope fields = spec.scope().fields();
        List<String> projection = fields.unrestrictedRead() && defaultFields.unrestrictedRead()
                ? readable
                : readable.stream().filter(field -> fields.canRead(field) && defaultFields.canRead(field)).toList();
        if (projection.isEmpty()) {
            throw new MappingException("entity has no selectable fields: " + metadata.type().getName());
        }
        return spec.withProjection(projection, List.of());
    }
}
