package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRequest;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaLiteralDdlPlanTest {
    @Test
    void rejectsBoundParametersInLiteralDdlPlans() {
        DynamicForm target = DynamicForm.builder("orders", "orders")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .build();

        assertThrows(IllegalArgumentException.class, () -> reviewed(target, List.of(
                new SqlRequest("alter table orders add label varchar(20) default ?", List.of("new")))));
    }

    private static SchemaMigrationPlan reviewed(DynamicForm target, List<SqlRequest> requests) {
        return new SchemaMigrationPlan(target.toTableDefinition(), true, requests, List.of());
    }
}
