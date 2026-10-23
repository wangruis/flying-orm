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
 * 批量更新的一行数据，乐观锁按需启用。
 *
 * <p>普通更新保存赋值和业务条件；带 lock 的更新额外绑定旧版本，并严格检查逐行影响数。
 * 同一批次不能混用普通更新和乐观锁更新，避免混淆结果语义。</p>
 *
 * @param values 待更新字段值
 * @param where 本行的业务条件
 * @param lock 可选乐观锁，null 表示普通更新
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public record BatchUpdate(Map<String, Object> values,
                          ConditionGroup where,
                          OptimisticLockOptions lock) {

    /** 创建不启用乐观锁的普通批量更新行。 */
    public BatchUpdate(Map<String, Object> values, ConditionGroup where) {
        this(values, where, null);
    }

    public BatchUpdate {
        Map<String, Object> safeValues = Objects.requireNonNull(values, "batch update values must not be null");
        if (safeValues.isEmpty()) {
            throw new IllegalArgumentException("batch update values must not be empty");
        }
        values = snapshotValues(safeValues);
        where = Objects.requireNonNull(where, "batch update where must not be null");
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
        Map<String, Object> snapshot = LinkedHashMap.newLinkedHashMap(source.size());
        source.forEach((name, value) -> snapshot.put(name, BindableValueSnapshots.logicalValue(value)));
        return Collections.unmodifiableMap(snapshot);
    }
}
