package com.flying.orm.rdb.repository;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.form.BatchUpdate;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.MappingException;
import org.reactivestreams.Publisher;

import java.util.Map;
import java.util.function.Function;

/** 同步和响应式批量共用的实体取值、请求装配规则；不持有执行状态。 */
enum RepositoryBatchKind {
    INSERT, UPSERT, INSERT_IF_ABSENT, UPDATE;

    <T> Function<T, ?> mapper(EntityMetadata<T> metadata, EntityValues<T> values,
                              RepositoryEntityIdSupport<T> ids, boolean explicitConflict) {
        if (this == INSERT_IF_ABSENT && ids.databaseGenerated() && !explicitConflict) {
            throw new MappingException("repository upsert does not support AUTO or sequence-generated primary keys");
        }
        return switch (this) {
            case UPDATE -> entity -> RepositoryUpdateSupport.batchUpdate(metadata, values, entity);
            case UPSERT -> entity -> {
                ids.prepare(entity, !explicitConflict);
                return !explicitConflict && ids.requiresGeneratedKey(entity)
                        ? values.readForInsert(entity) : values.repositoryUpsertValues(entity);
            };
            case INSERT, INSERT_IF_ABSENT -> entity -> {
                ids.prepare(entity);
                return values.readForInsert(entity);
            };
        };
    }

    /** 在调用边界解析并快照属性名，逐行映射不重复解析冲突列。 */
    static String[] conflictColumns(EntityMetadata<?> metadata, String[] properties) {
        if (properties == null || properties.length == 0) return null;
        String[] columns = new String[properties.length];
        for (int index = 0; index < columns.length; index++) {
            columns[index] = metadata.field(properties[index]).columnName();
        }
        return columns;
    }

    EntityLifecyclePhase before() {
        return this == UPDATE ? EntityLifecyclePhase.PRE_UPDATE : EntityLifecyclePhase.PRE_PERSIST;
    }

    EntityLifecyclePhase after() {
        return this == UPDATE ? EntityLifecyclePhase.POST_UPDATE : EntityLifecyclePhase.POST_PERSIST;
    }

    @SuppressWarnings("unchecked")
    BatchSpec spec(DynamicForm form, Publisher<?> rows, BatchWriteOptions options,
                   DataScope scope, BatchGeneratedKeys generatedKeys, String[] conflictColumns) {
        BatchSpec spec = switch (this) {
            case INSERT -> BatchSpec.insert(form, (Publisher<Map<String, Object>>) rows);
            case UPSERT -> BatchSpec.upsert(form, (Publisher<Map<String, Object>>) rows);
            case INSERT_IF_ABSENT -> BatchSpec.upsert(form, (Publisher<Map<String, Object>>) rows).ignoreConflicts();
            case UPDATE -> BatchSpec.update(form, (Publisher<BatchUpdate>) rows);
        };
        if (conflictColumns != null) spec = spec.onConflict(conflictColumns);
        if (scope != null) {
            spec = spec.withScope(scope);
        }
        spec = spec.withOptions(options);
        return this == INSERT || this == UPSERT ? spec.withGeneratedKeys(generatedKeys) : spec;
    }
}
