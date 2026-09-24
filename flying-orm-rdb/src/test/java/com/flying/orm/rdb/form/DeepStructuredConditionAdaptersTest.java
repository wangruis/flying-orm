package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.array.ArrayConditionValue;
import com.flying.orm.rdb.array.ArrayStructuredConditions;
import com.flying.orm.rdb.json.JsonConditionValue;
import com.flying.orm.rdb.json.JsonStructuredConditions;
import com.flying.orm.rdb.vector.VectorStructuredConditions;
import com.flying.orm.rdb.vector.VectorTermHandlers;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

class DeepStructuredConditionAdaptersTest {

    private static final int DEPTH = 10_000;

    @Test
    void jsonAdapterReachesALeafUnderTenThousandGroups() {
        DynamicForm form = form("JSON");
        StructuredConditionInput input = deep(StructuredConditionInput.term("value", "json-path-eq",
                Map.of("path", "id", "value", 7)));

        StructuredConditionInput adapted = JsonStructuredConditions.standard().adapt(form, input);

        assertInstanceOf(JsonConditionValue.class, leaf(adapted).value());
        assertEquals("json-path-eq", leaf(adapted).operator());
    }

    @Test
    void arrayAdapterReachesALeafUnderTenThousandGroups() {
        DynamicForm form = form("INTEGER[]");
        StructuredConditionInput input = deep(StructuredConditionInput.term("value", "array-contains", List.of(7)));

        StructuredConditionInput adapted = ArrayStructuredConditions.postgresql().adapt(form, input);

        assertInstanceOf(ArrayConditionValue.class, leaf(adapted).value());
        assertEquals("array-contains", leaf(adapted).operator());
    }

    @Test
    void vectorAdapterReachesALeafUnderTenThousandGroups() {
        DynamicForm form = form("VECTOR");
        StructuredConditionInput input = deep(StructuredConditionInput.term("value", "vector-l2-lt",
                Map.of("vector", List.of(1F, 2F), "distance", 3D)));

        StructuredConditionInput adapted = VectorStructuredConditions.postgresql().adapt(form, input);

        StructuredConditionInput leaf = leaf(adapted);
        assertEquals("vector-l2-lt", leaf.operator());
        ConditionGroup where = ConditionGroup.and().add(TermCondition.of(leaf.field(), leaf.operator(), leaf.value())).build();
        assertEquals(2, SqlRenderer.builder().addTermPackage(VectorTermHandlers.postgresql()).build()
                .renderWhere(where).parameters().size());
    }

    @Test
    void unrelatedTermsKeepTheirIdentityAndMixedShapeValidationStaysWithTheCompiler() {
        StructuredConditionInput input = deep(StructuredConditionInput.term("value", "eq", 7));
        StructuredConditionInput mixed = new StructuredConditionInput("value", "eq", 7, "and",
                java.util.Collections.singletonList(null));
        for (StructuredConditionCustomizer customizer : List.of(JsonStructuredConditions.standard(),
                ArrayStructuredConditions.postgresql(), VectorStructuredConditions.postgresql())) {
            assertSame(input, customizer.adapt(form("INTEGER"), input));
            assertSame(mixed, customizer.adapt(form("INTEGER"), mixed));
        }
    }

    private static DynamicForm form(String type) {
        return DynamicForm.builder("deep_adapter", "deep_adapter")
                .addField(DynamicField.of("value", type)).build();
    }

    private static StructuredConditionInput deep(StructuredConditionInput leaf) {
        StructuredConditionInput result = leaf;
        for (int depth = 0; depth < DEPTH; depth++) result = StructuredConditionInput.and(result);
        return result;
    }

    private static StructuredConditionInput leaf(StructuredConditionInput input) {
        for (int depth = 0; depth < DEPTH; depth++) {
            assertEquals("and", input.logic());
            assertEquals(1, input.terms().size());
            input = input.terms().getFirst();
        }
        return input;
    }
}
