package com.flying.orm.rdb.repository;

import com.flying.orm.core.page.PageSort;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.MappingException;

import java.util.Comparator;
import java.util.List;

/** 把实体的默认投影和默认排序收口到同一处，保证 JDBC 与 R2DBC Repository 行为一致。 */
final class RepositoryQueryDefaults {

    private RepositoryQueryDefaults() {
    }

    static QuerySpec apply(QuerySpec spec, EntityMetadata<?> metadata) {
        QuerySpec projected = applyProjection(spec, metadata);
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

    private static QuerySpec applyProjection(QuerySpec spec, EntityMetadata<?> metadata) {
        List<String> readable = metadata.fields().stream()
                .filter(EntityFieldMetadata::selectable)
                .map(EntityFieldMetadata::columnName)
                .toList();
        if (readable.size() == metadata.fields().size()) {
            return spec;
        }
        if (readable.isEmpty()) {
            throw new MappingException("entity has no selectable fields: " + metadata.type().getName());
        }
        return spec.withProjection(readable, List.of());
    }
}
