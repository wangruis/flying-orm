package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

class FormFieldDecodingPlanCacheTest {

    @Test
    void ordinaryFormsShareTheEmptyDecodingPlan() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
        DynamicForm form = DynamicForm.builder("items", "items")
                                      .addField(DynamicField.of("name", "VARCHAR"))
                                      .build();

        FormFieldDecodingPlan first = FormFieldDecodingPlan.compile(form, renderer);
        FormFieldDecodingPlan second = FormFieldDecodingPlan.compile(form, renderer);

        assertSame(first, second);
    }

    @Test
    void reusesAFormStructurePlanAcrossEquivalentFormInstancesAndInvalidatesWithMetadata() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
        DynamicForm firstForm = DynamicForm.builder("items", "items")
                                           .addField(DynamicField.of("payload", "JSON"))
                                           .build();
        DynamicForm secondForm = DynamicForm.builder("items", "items")
                                            .addField(DynamicField.of("payload", "JSON"))
                                            .build();
        FormFieldDecodingPlan first = renderer.resultDecodingPlan(firstForm);
        FormFieldDecodingPlan second = renderer.resultDecodingPlan(secondForm);

        assertSame(first, second);
        renderer.resultPlanInvalidator().invalidate("items");
        assertNotSame(first, renderer.resultDecodingPlan(secondForm));
    }
}
