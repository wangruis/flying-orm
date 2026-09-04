package com.flying.orm.core.form;

import java.util.AbstractList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * DynamicFormChangeSet 描述动态表单版本之间可执行的结构变更集合。
 *
 * @param source        原表单定义
 * @param target        目标表单定义
 * @param addedFields   新增字段
 * @param removedFields 删除字段
 * @param changedFields 变更字段
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public record DynamicFormChangeSet(DynamicForm source,
                                   DynamicForm target,
                                   List<DynamicField> addedFields,
                                   List<DynamicField> removedFields,
                                   List<FieldChange> changedFields) {

    /**
     * 创建动态表单变更集并发布只读集合。
     *
     * @param source        原表单定义
     * @param target        目标表单定义
     * @param addedFields   新增字段
     * @param removedFields 删除字段
     * @param changedFields 变更字段
     */
    public DynamicFormChangeSet {
        source = Objects.requireNonNull(source, "source form must not be null");
        target = Objects.requireNonNull(target, "target form must not be null");
        boolean owned = addedFields instanceof OwnedList<?>
                && removedFields instanceof OwnedList<?>
                && changedFields instanceof OwnedList<?>;
        addedFields = unwrapOrCopy(addedFields, "added fields must not be null");
        removedFields = unwrapOrCopy(removedFields, "removed fields must not be null");
        changedFields = unwrapOrCopy(changedFields, "changed fields must not be null");
        if (!source.mapsToSameRelation(target)) {
            throw new IllegalArgumentException("form change set must target the same physical table");
        }
        if (!owned && (!addedFields.equals(expectedAdded(source, target))
                || !removedFields.equals(expectedRemoved(source, target))
                || !changedFields.equals(expectedChanged(source, target)))) {
            throw new IllegalArgumentException("form change set does not match source and target definitions");
        }
    }

    /** 包内 diff 算法发布刚计算完成且不会再修改的变更列表，不重复推导或复制。 */
    static DynamicFormChangeSet ownedDiff(DynamicForm source,
                                          DynamicForm target,
                                          List<DynamicField> addedFields,
                                          List<DynamicField> removedFields,
                                          List<FieldChange> changedFields) {
        return new DynamicFormChangeSet(
                source,
                target,
                new OwnedList<>(addedFields),
                new OwnedList<>(removedFields),
                new OwnedList<>(changedFields));
    }

    /**
     * 判断变更集是否为空。
     *
     * @return 没有结构变更时返回 true
     */
    public boolean isEmpty() {
        return addedFields.isEmpty() && removedFields.isEmpty() && changedFields.isEmpty();
    }

    private static List<DynamicField> expectedAdded(DynamicForm source, DynamicForm target) {
        return target.fields().stream()
                     .filter(field -> source.findField(field.name()).isEmpty())
                     .toList();
    }

    private static List<DynamicField> expectedRemoved(DynamicForm source, DynamicForm target) {
        return source.fields().stream()
                     .filter(field -> target.findField(field.name()).isEmpty())
                     .toList();
    }

    private static List<FieldChange> expectedChanged(DynamicForm source, DynamicForm target) {
        return target.fields().stream()
                     .map(field -> source.findField(field.name())
                                         .filter(sourceField -> !sourceField.equals(field))
                                         .map(sourceField -> new FieldChange(sourceField, field))
                                         .orElse(null))
                     .filter(Objects::nonNull)
                     .toList();
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> unwrapOrCopy(List<T> values, String message) {
        List<T> safeValues = Objects.requireNonNull(values, message);
        return safeValues instanceof OwnedList<?> owned
                ? (List<T>) owned.values()
                : List.copyOf(safeValues);
    }

    private static final class OwnedList<T> extends AbstractList<T> {

        private final List<T> values;

        private OwnedList(List<T> values) {
            this.values = Collections.unmodifiableList(Objects.requireNonNull(values, "owned values must not be null"));
        }

        @Override
        public T get(int index) {
            return values.get(index);
        }

        @Override
        public int size() {
            return values.size();
        }

        private List<T> values() {
            return values;
        }
    }
}
