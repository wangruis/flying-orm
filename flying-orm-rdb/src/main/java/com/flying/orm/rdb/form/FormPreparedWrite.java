package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime.ContainsFieldTokens;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Package-owned carrier between scope/protection and SQL rendering. */
record FormPreparedWrite(DynamicForm physicalForm,
                         Map<String, Object> values,
                         List<ContainsFieldTokens> containsTokens) {
    FormPreparedWrite {
        Objects.requireNonNull(physicalForm, "physical form must not be null");
        Objects.requireNonNull(values, "prepared write values must not be null");
        containsTokens = List.copyOf(containsTokens);
    }
}
