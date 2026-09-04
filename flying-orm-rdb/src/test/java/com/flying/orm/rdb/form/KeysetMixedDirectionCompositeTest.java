package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorPosition;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.core.sql.render.SqlFragment;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KeysetMixedDirectionCompositeTest {

    @Test
    void rendersPortableLexicographicBranchesWithoutTupleComparison() {
        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.of("priority", "INTEGER").withNullable(false))
                .addField(DynamicField.of("created_at", "TIMESTAMP").withNullable(false))
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .build();
        KeysetPageNormalizer.NormalizedKeysetPage page = KeysetPageNormalizer.normalize(
                form,
                KeysetPageQuery.after(
                        50,
                        CursorPosition.of(List.of(5, "2026-09-03T00:00:00", 9L)),
                        KeysetSort.desc("priority", NullOrder.LAST),
                        KeysetSort.asc("created_at", NullOrder.FIRST)));

        SqlFragment predicate = KeysetPredicateRenderer.render(
                page, field -> '"' + field + '"', (field, value) -> value);

        assertEquals("(\"priority\" < ? OR (\"priority\" = ? AND \"created_at\" > ?)"
                             + " OR (\"priority\" = ? AND \"created_at\" = ? AND \"id\" > ?))",
                     predicate.sql());
        assertEquals(List.of(5, 5, "2026-09-03T00:00:00", 5, "2026-09-03T00:00:00", 9L),
                     predicate.parameters());
    }
}
