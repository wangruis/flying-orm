package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import org.junit.jupiter.api.Test;

import java.util.AbstractList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaMigrationPlanTest {

    @Test
    void validatesAndPublishesTheSameAdditionalTableSnapshot() {
        DynamicForm target = DynamicForm.builder("orders", "orders")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .build();
        List<String> changingSource = new AbstractList<>() {
            private int reads;

            @Override
            public String get(int index) {
                if (index != 0) {
                    throw new IndexOutOfBoundsException(index);
                }
                return reads++ == 0 ? "audit_aux" : " ";
            }

            @Override
            public int size() {
                return 1;
            }
        };

        SchemaMigrationPlan plan = new SchemaMigrationPlan(
                target,
                List.of(),
                List.of(),
                true,
                List.of(),
                List.of(),
                changingSource);

        assertEquals(List.of("audit_aux"), plan.additionalCreatedTables());
    }

    @Test
    void rejectsBlankAdditionalCreatedTableAtThePublicPlanBoundary() {
        DynamicForm target = DynamicForm.builder("orders", "orders")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .build();

        assertThrows(IllegalArgumentException.class, () -> new SchemaMigrationPlan(
                target,
                List.of(),
                List.of(),
                true,
                List.of(),
                List.of(),
                List.of(" ")));
    }
}
