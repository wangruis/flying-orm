package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.rdb.internal.InternalApi;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Repository upsert 的阶段化实体快照。
 *
 * <p>Map 视图保持现有批量管线需要的稳定字段并集；INSERT 与冲突 UPDATE 各自使用独立快照，
 * SQL 计划不再从并集反推阶段语义。实例不可变，可安全穿过冷 Publisher。</p>
 *
 * @author wangr
 * @date 2026-08-28
 * @version v1.0
 */
@InternalApi
public final class RepositoryUpsertValues extends AbstractMap<String, Object> {

    private final Map<String, Object> values;
    private final Map<String, Object> insertValues;
    private final Map<String, Object> updateValues;

    /** 接管 EntityValues 为当前快照新建且不再修改的三张有序 Map。 */
    RepositoryUpsertValues(Map<String, Object> values,
                           Map<String, Object> insertValues,
                           Map<String, Object> updateValues) {
        this.values = takeOwnership(values, "repository upsert values must not be null");
        this.insertValues = takeOwnership(insertValues, "repository upsert insert values must not be null");
        this.updateValues = takeOwnership(updateValues, "repository upsert update values must not be null");
    }

    /** @return 只服从 insert strategy 的字段快照。 */
    public Map<String, Object> insertValues() {
        return insertValues;
    }

    /** @return 只服从 update strategy 的冲突更新字段快照。 */
    public Map<String, Object> updateValues() {
        return updateValues;
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return values.entrySet();
    }

    private static Map<String, Object> takeOwnership(Map<String, Object> values, String message) {
        return Collections.unmodifiableMap(Objects.requireNonNull(values, message));
    }
}
