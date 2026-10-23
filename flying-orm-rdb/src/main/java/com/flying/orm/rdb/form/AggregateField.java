package com.flying.orm.rdb.form;



import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinFieldRef;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUseRequirements;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.core.scope.FieldVisibility;

/** 聚合输出的原始字段身份；结果别名不替代 codec、Scope 或脱敏来源。 */
record AggregateField(DynamicForm form, DynamicField field, JoinFieldRef reference, String expression) {

    void requireUse(FieldUseRequirements.Builder requirements, FieldUse use) {
        if (requirements == null) {
            return;
        }
        if (reference == null) {
            requirements.require(field.name(), use);
        } else {
            requirements.requireJoin(reference, use);
        }
    }

    FieldVisibility visibility(FieldUseSnapshot snapshot) {
        return reference == null ? snapshot.visibility(field.name()) : snapshot.joinVisibility(reference);
    }
}
