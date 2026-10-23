package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class FormAggregateMaskingTest {
    @Test
    void singleValueMatchesRowMaskingForEveryDisplayMode() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        FormAggregateReadSupport reads = new FormAggregateReadSupport(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.all()));
        ProtectedFieldRuntime masking = ProtectedFieldRuntime.withoutKeys();
        for (SensitiveDisplayMode declared : new SensitiveDisplayMode[]{
                SensitiveDisplayMode.FULL, SensitiveDisplayMode.MASKED}) {
            DynamicForm form = DynamicForm.builder("secrets", "secrets")
                    .addField(DynamicField.of("secret", "VARCHAR"))
                    .masked("secret", MaskedFieldDefinition.builder("full").display(declared).build()).build();
            for (SensitiveDisplayMode requested : SensitiveDisplayMode.values()) {
                for (Object value : new Object[]{null, "", "classified", 12, new StringBuilder("text")}) {
                    DynamicRow row = DynamicRow.copyOf(Collections.singletonMap("secret", value));
                    Object expected;
                    try {
                        expected = masking.maskResult(form, row, requested).get("secret");
                    } catch (IllegalArgumentException error) {
                        IllegalArgumentException actual = assertThrows(IllegalArgumentException.class,
                                () -> reads.maskGroupValue(form, form.field("secret"), value, requested));
                        assertEquals(error.getMessage(), actual.getMessage());
                        continue;
                    }
                    assertEquals(expected, reads.maskGroupValue(form, form.field("secret"), value, requested));
                }
            }
        }
    }

    @Test
    void absentMaskOrNullValueNeedsNoDisplayConversion() {
        FormAggregateReadSupport reads = new FormAggregateReadSupport(FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql()),
                new FormScopeSupport(FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql()), StructuredConditionResolver.defaults(), DataScope.all()));
        DynamicForm form = DynamicForm.builder("plain", "plain")
                .addField(DynamicField.of("name", "VARCHAR")).build();
        StringBuilder value = new StringBuilder("unmasked");
        assertSame(value, reads.maskGroupValue(form, form.field("name"), value, null));
        assertNull(reads.maskGroupValue(form, form.field("name"), null, null));
    }
}
