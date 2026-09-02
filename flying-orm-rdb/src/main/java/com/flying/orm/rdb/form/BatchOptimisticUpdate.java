package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.internal.value.BindableValueSnapshots;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.lock.OptimisticLockOptions;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 批量乐观更新里的一行数据。
 *
 * <p>每一行都有自己的更新值、业务条件和旧版本值。这样执行器才能准确知道到底是哪一行发生了并发冲突，
 * 而不是只看到整批影响行数少了几行。</p>
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public record BatchOptimisticUpdate(Map<String, Object> values,
                                    ConditionGroup where,
                                    OptimisticLockOptions lock) {

    public BatchOptimisticUpdate {
        Map<String, Object> safeValues = Objects.requireNonNull(values, "batch update values must not be null");
        if (safeValues.isEmpty()) {
            throw new IllegalArgumentException("batch update values must not be empty");
        }
        values = snapshotValues(safeValues);
        where = Objects.requireNonNull(where, "batch update where must not be null");
        lock = Objects.requireNonNull(lock, "batch update lock must not be null");
    }

    /**
     * 返回构造时固定的待更新字段；可变可绑定值在每次读取时返回不可变快照，防止冷批量 Publisher
     * 在订阅前被调用方改写。
     *
     * @return 不可修改的字段 Map
     */
    @Override
    public Map<String, Object> values() {
        return snapshotValues(values);
    }

    /** Internal planning seam for values already owned by this immutable batch row. */
    @InternalApi
    Map<String, Object> ownedValues() {
        return values;
    }

    /** 内部联合预算：直接遍历保存值与其他保留值，不导出可变载荷，也不包含行对象的固定开销。 */
    @InternalApi
    public long estimatedRetainedBytes(Object retainedValues) {
        return BatchMemoryBudget.estimateValueBytes(new Object[]{retainedValues, values});
    }

    /**
     * 冻结标准可绑定的可变值，普通业务对象仍保持既有可信交接语义。
     */
    private static Map<String, Object> snapshotValues(Map<String, Object> source) {
        Map<String, Object> snapshot = new LinkedHashMap<>(source.size());
        source.forEach((name, value) -> snapshot.put(name, BindableValueSnapshots.immutableValue(value)));
        return Collections.unmodifiableMap(snapshot);
    }
}
