package com.flying.orm.rdb.form.spec;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageSort;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 查询规格只在调用方输入进入不可变边界时冻结列表。 */
class QuerySpecInputOwnershipTest {

    @Test
    void snapshotsProjectionGroupAndSortInputsAtTheirReplacementBoundaries() {
        List<String> projections = new ArrayList<>(List.of("id", "name"));
        List<String> groups = new ArrayList<>(List.of("name"));
        List<PageSort> sorts = new ArrayList<>(List.of(PageSort.asc("name")));

        QuerySpec spec = QuerySpec.of(form(), ConditionGroup.and().build())
                .withProjection(projections, groups)
                .withSorts(sorts);
        projections.add("created_at");
        groups.clear();
        sorts.add(PageSort.desc("id"));

        assertEquals(List.of("id", "name"), spec.projections());
        assertEquals(List.of("name"), spec.groups());
        assertEquals(List.of(PageSort.asc("name")), spec.sorts());
        assertThrows(UnsupportedOperationException.class, () -> spec.projections().add("created_at"));
        assertThrows(UnsupportedOperationException.class, () -> spec.groups().clear());
        assertThrows(UnsupportedOperationException.class, () -> spec.sorts().clear());
    }

    @Test
    void preservesProjectionGroupAndSortRejectionContracts() {
        QuerySpec spec = QuerySpec.of(form(), ConditionGroup.and().build());

        assertEquals("query projections must not be null",
                assertThrows(NullPointerException.class,
                        () -> spec.withProjection(null, List.of())).getMessage());
        assertEquals("projected query must select at least one field",
                assertThrows(IllegalArgumentException.class,
                        () -> spec.withProjection(List.of(), null)).getMessage());
        assertThrows(NullPointerException.class,
                () -> spec.withProjection(Arrays.asList("id", null), List.of()));
        assertEquals("query projections must not contain blank values",
                assertThrows(IllegalArgumentException.class,
                        () -> spec.withProjection(List.of(" "), List.of())).getMessage());
        assertEquals("query groups must not contain blank values",
                assertThrows(IllegalArgumentException.class,
                        () -> spec.withProjection(List.of("id", "id"), List.of("\t"))).getMessage());
        assertEquals("query sorts must not be null",
                assertThrows(NullPointerException.class, () -> spec.withSorts(null)).getMessage());
        assertThrows(NullPointerException.class,
                () -> spec.withSorts(Arrays.asList(PageSort.asc("id"), null)));
        assertEquals(List.of("id", "id"),
                spec.withProjection(List.of("id", "id"), List.of()).projections());
    }

    @Test
    void preservesFactoryAndStructuredPolicyNullPrecedence() {
        assertEquals("query form must not be null",
                assertThrows(NullPointerException.class, () -> QuerySpec.of(null, null)).getMessage());
        assertEquals("structured condition input must not be null",
                assertThrows(NullPointerException.class, () -> QuerySpec.structured(null, null)).getMessage());
        assertEquals("structured policy requires a structured query spec",
                assertThrows(IllegalStateException.class,
                        () -> QuerySpec.of(form(), ConditionGroup.and().build())
                                .withStructuredPolicy(null)).getMessage());
    }

    private static DynamicForm form() {
        return DynamicForm.builder("records", "records")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR"))
                .addField(DynamicField.of("created_at", "TIMESTAMP"))
                .build();
    }
}
