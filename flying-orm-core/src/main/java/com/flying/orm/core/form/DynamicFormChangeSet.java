package com.flying.orm.core.form;

import java.util.Collections;
import java.util.List;

/**
 * 由 {@link DynamicForm#diffTo(DynamicForm)} 计算的结构变更快照。
 * 变更事实只来自两个表单，不接受调用方另外维护一份差异列表。
 *
 * @author wangr
 * @version v4.1.2
 */
public final class DynamicFormChangeSet {

    private final DynamicForm source;
    private final DynamicForm target;
    private final List<DynamicField> addedFields;
    private final List<DynamicField> removedFields;
    private final List<FieldChange> changedFields;

    /** 接管 diff 算法本次新建的列表；发布后不再修改，不重复计算或复制。 */
    DynamicFormChangeSet(DynamicForm source, DynamicForm target,
                         List<DynamicField> addedFields, List<DynamicField> removedFields,
                         List<FieldChange> changedFields) {
        this.source = source;
        this.target = target;
        this.addedFields = Collections.unmodifiableList(addedFields);
        this.removedFields = Collections.unmodifiableList(removedFields);
        this.changedFields = Collections.unmodifiableList(changedFields);
    }

    public DynamicForm source() { return source; }

    public DynamicForm target() { return target; }

    public List<DynamicField> addedFields() { return addedFields; }

    public List<DynamicField> removedFields() { return removedFields; }

    public List<FieldChange> changedFields() { return changedFields; }

    /** @return 没有结构变更时为 true */
    public boolean isEmpty() {
        return addedFields.isEmpty() && removedFields.isEmpty() && changedFields.isEmpty();
    }
}
