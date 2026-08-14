package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.rdb.internal.MutableValueSnapshots;
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
     * 返回构造时固定的待更新字段；任意数组值在每次读取时返回新的数组图副本，防止冷批量 Publisher 在订阅前被调用方改写。
     *
     * @return 不可修改的字段 Map；非数组值保持既有引用语义，数组值为新的数组图副本
     */
    @Override
    public Map<String, Object> values() {
        return snapshotValues(values);
    }

    /**
     * 只沿数组节点复制完整数组图，既冻结可变二进制/驱动数组边界，也不改变普通业务对象的既有所有权。
     */
    private static Map<String, Object> snapshotValues(Map<String, Object> source) {
        Map<String, Object> snapshot = new LinkedHashMap<>(source.size());
        source.forEach((name, value) -> snapshot.put(name, MutableValueSnapshots.arrayGraph(value)));
        return Collections.unmodifiableMap(snapshot);
    }
}
