package com.flying.orm.rdb.vector;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VectorQueryLimitTest {
    @Test
    void nearestQueryUsesTheDeveloperLimitWithoutAFixedCeiling() {
        DynamicForm form = DynamicForm.builder("vectors", "vectors")
                .addField(DynamicField.primaryKey("id", "INTEGER"))
                .addField(DynamicField.of("embedding", "VECTOR")).build();
        PostgresqlVectorQueryRenderer renderer = PostgresqlVectorQueryRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build());
        for (int limit : new int[]{10_001, Integer.MAX_VALUE}) {
            SqlRequest request = renderer.nearest(form, List.of("id"), "embedding", new float[]{1, 2},
                    VectorMetric.L2, ConditionGroup.and().build(), limit);
            assertEquals(limit, request.parameters().getLast());
        }
        assertThrows(IllegalArgumentException.class, () -> renderer.nearest(form, List.of("id"), "embedding",
                new float[]{1, 2}, VectorMetric.L2, ConditionGroup.and().build(), 0));
    }
}
