package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;

import java.util.Map;
import java.util.Objects;

/** Package-owned carrier between scope/protection and SQL rendering. */
final class FormPreparedWrite {

    private final DynamicForm physicalForm;

    private final Map<String, Object> values;

    FormPreparedWrite(DynamicForm physicalForm, Map<String, Object> values) {
        this.physicalForm = Objects.requireNonNull(physicalForm, "physical form must not be null");
        this.values = Objects.requireNonNull(values, "prepared write values must not be null");
    }

    DynamicForm physicalForm() {
        return physicalForm;
    }

    Map<String, Object> values() {
        return values;
    }
}
