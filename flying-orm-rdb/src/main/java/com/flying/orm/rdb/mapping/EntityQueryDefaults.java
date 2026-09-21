package com.flying.orm.rdb.mapping;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
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
     * <p>同步与响应式入口只负责各自的执行和结果生命周期；逻辑删除、默认投影、默认排序、Scope
     * 与执行保护在这里装配一次，避免两条入口以后产生语义漂移。</p>
     */
    public static QuerySpec query(DynamicForm form,
                                  EntityMetadata<?> metadata,
                                  ConditionGroup where,
                                  DataScope scope,
                                  SqlExecutionOptions options) {
        return query(form, metadata, where, scope, options, DataScope.none().fields());
    }

    /** 默认投影在请求字段范围和客户端默认字段范围内生成，不能提前申请已被裁掉的列。 */
    public static QuerySpec query(DynamicForm form,
                                  EntityMetadata<?> metadata,
                                  ConditionGroup where,
                                  DataScope scope,
                                  SqlExecutionOptions options,
                                  FieldScope defaultFields) {
        QuerySpec spec = QuerySpec.of(form, where);
        if (scope != null) {
            spec = spec.withScope(scope);
        }
        spec = apply(spec, metadata, defaultFields);
        return options == null ? spec : spec.withExecutionOptions(options);
    }

    private static QuerySpec apply(QuerySpec spec, EntityMetadata<?> metadata, FieldScope defaultFields) {
        QuerySpec projected = applyProjection(spec, metadata, defaultFields);
        if (!projected.sorts().isEmpty()) {
            return projected;
        }
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
    public static QuerySpec applyProjection(QuerySpec spec, EntityMetadata<?> metadata) {
        return applyProjection(spec, metadata, DataScope.none().fields());
    }

    /** 只裁剪实体的隐式投影，不把默认排除列变成字段权限，也不改动显式 select。 */
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
