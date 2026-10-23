package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 动态表单与实体插入共用的阶段化值，保留 UPSERT 的 INSERT/UPDATE 字段归属。
 *
 * <p>Map 视图保持现有批量管线需要的稳定字段并集；INSERT 与冲突 UPDATE 各自使用独立快照，
 * SQL 计划不再从并集反推阶段语义。实例不可变，可安全穿过冷 Publisher。</p>
 *
 * @author wangr
 * @date 2026-08-28
 * @version v1.0
 */
public final class RepositoryUpsertValues extends AbstractMap<String, Object> {

    private final Map<String, Object> values;
    private final Map<String, Object> insertValues;
    private final Map<String, Object> updateValues;
    private final Object layoutIdentity;

    /** 接管本次快照独占的有序 Map；相同阶段复用同一只读视图。 */
    RepositoryUpsertValues(Map<String, Object> values,
                           Map<String, Object> insertValues,
                           Map<String, Object> updateValues) {
        this(values, insertValues, updateValues, null);
    }

    /** 固定策略使用不持有映射计划的共享标记，条件策略逐行比较真实阶段字段。 */
    RepositoryUpsertValues(Map<String, Object> values,
                           Map<String, Object> insertValues,
                           Map<String, Object> updateValues,
                           Object layoutIdentity) {
        this.values = takeOwnership(values, "repository upsert values must not be null");
        this.insertValues = insertValues == values ? this.values
                : takeOwnership(insertValues, "repository upsert insert values must not be null");
        this.updateValues = updateValues == values ? this.values : updateValues == insertValues ? this.insertValues
                : takeOwnership(updateValues, "repository upsert update values must not be null");
        this.layoutIdentity = layoutIdentity;
    }

    /** 生成显式应用默认值并收敛阶段字段；无默认值/排除配置时复用输入，不修改调用方的行。 */
    public static Map<String, Object> prepareInsert(DynamicForm form, Map<String, Object> row,
                                                    boolean insertOnly, Set<String> ignoredUpdates) {
        row = applyDefaults(form, row, insertOnly);
        RepositoryUpsertValues staged = row instanceof RepositoryUpsertValues values ? values : null;
        if (insertOnly) return staged == null ? row : staged.insertValues;
        if (ignoredUpdates.isEmpty()) return row;
        Map<String, Object> values = new LinkedHashMap<>();
        Map<String, Object> inserts = staged == null ? values : new LinkedHashMap<>();
        Map<String, Object> updates = new LinkedHashMap<>();
        for (Entry<String, Object> entry : row.entrySet()) {
            DynamicField field = form.field(entry.getKey());
            boolean insert = staged == null || staged.insertValues.containsKey(entry.getKey());
            boolean update = (staged == null || staged.updateValues.containsKey(entry.getKey()))
                    && !ignoredUpdates.contains(field.normalizedName());
            if (!insert && !update) continue;
            String name = field.name();
            if (values.containsKey(name)) throw new IllegalArgumentException("duplicate normalized batch insert field");
            values.put(name, entry.getValue());
            if (insert && inserts != values) inserts.put(name, entry.getValue());
            if (update) updates.put(name, entry.getValue());
        }
        return new RepositoryUpsertValues(values, inserts, updates);
    }

    /** 应用默认值只属于本次 INSERT；不修改调用者输入或污染 UPSERT 的 UPDATE 阶段。 */
    private static Map<String, Object> applyDefaults(DynamicForm form, Map<String, Object> row, boolean insertOnly) {
        if (form.runtimeDefaultFields().isEmpty()) return row;
        RepositoryUpsertValues staged = row instanceof RepositoryUpsertValues values ? values : null;
        Map<String, Object> insert = staged == null ? row : staged.insertValues;
        Map<String, Object> values = null;
        Map<String, Object> inserts = null;
        Map<String, Object> updates = null;
        Map<String, String> suppliedNames = null;
        for (DynamicField field : form.runtimeDefaultFields()) {
            String name = field.name();
            if (!insert.containsKey(name)) {
                if (suppliedNames == null) {
                    suppliedNames = new HashMap<>();
                    for (String supplied : insert.keySet()) {
                        suppliedNames.put(form.field(supplied).normalizedName(), supplied);
                    }
                }
                name = suppliedNames.getOrDefault(field.normalizedName(), name);
            }
            // 已有阶段快照明确排除的 INSERT 字段，不由模型默认值重新加入。
            if (staged != null && !insert.containsKey(name)) continue;
            if (insert.get(name) != null) continue;
            Object value = field.runtimeDefault().get();
            if (value == null) continue;
            if (values == null) {
                values = new LinkedHashMap<>(insertOnly ? insert : row);
                if (!insertOnly) {
                    inserts = new LinkedHashMap<>(insert);
                    updates = new LinkedHashMap<>(staged == null ? row : staged.updateValues);
                }
            }
            values.put(name, value);
            if (!insertOnly) {
                inserts.put(name, value);
                updates.remove(name);
            }
        }
        return values == null ? row : insertOnly ? values : new RepositoryUpsertValues(values, inserts, updates);
    }

    /** 相邻行能否复用同一 SQL；阶段字段必须分别比较，不能只比较并集。 */
    public static boolean sameLayout(Map<String, Object> first, Map<String, Object> row) {
        if (first instanceof RepositoryUpsertValues expected) {
            return row instanceof RepositoryUpsertValues actual
                    && (expected.layoutIdentity != null && expected.layoutIdentity == actual.layoutIdentity
                    || expected.insertValues.keySet().equals(actual.insertValues.keySet())
                    && expected.updateValues.keySet().equals(actual.updateValues.keySet()));
        }
        return !(row instanceof RepositoryUpsertValues) && first.keySet().equals(row.keySet());
    }

    /** @return 只服从 insert strategy 的字段快照。 */
    public Map<String, Object> insertValues() {
        return insertValues;
    }

    /** @return 只服从 update strategy 的冲突更新字段快照。 */
    public Map<String, Object> updateValues() {
        return updateValues;
    }

    /** 替换已经快照的值，同时保留 INSERT/UPDATE 的字段归属和固定布局标记。 */
    public RepositoryUpsertValues withSnapshot(Map<String, Object> snapshot) {
        Map<String, Object> inserts = stageSnapshot(insertValues, snapshot);
        Map<String, Object> updates = updateValues == insertValues ? inserts : stageSnapshot(updateValues, snapshot);
        return new RepositoryUpsertValues(snapshot, inserts, updates, layoutIdentity);
    }

    private Map<String, Object> stageSnapshot(Map<String, Object> stage, Map<String, Object> snapshot) {
        if (stage == values) return snapshot;
        Map<String, Object> result = new LinkedHashMap<>(stage.size());
        for (String field : stage.keySet()) result.put(field, snapshot.get(field));
        return result;
    }

    @Override
    public Object get(Object key) {
        return values.get(key);
    }

    @Override
    public boolean containsKey(Object key) {
        return values.containsKey(key);
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return values.entrySet();
    }

    private static Map<String, Object> takeOwnership(Map<String, Object> values, String message) {
        return Collections.unmodifiableMap(Objects.requireNonNull(values, message));
    }
}
