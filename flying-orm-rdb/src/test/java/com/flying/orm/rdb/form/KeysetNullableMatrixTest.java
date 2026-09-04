package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorPosition;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.core.sql.render.SqlFragment;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KeysetNullableMatrixTest {

    @Test
    void nullsLastContinuesInsideTheNullGroupUsingTheHiddenPrimaryKey() {
        DynamicForm form = form();
        KeysetPageNormalizer.NormalizedKeysetPage page = KeysetPageNormalizer.normalize(
                form,
                KeysetPageQuery.after(
                        20,
                        CursorPosition.of(Arrays.asList(null, 7L)),
                        KeysetSort.asc("score", NullOrder.LAST)));

        SqlFragment predicate = KeysetPredicateRenderer.render(
                page, KeysetNullableMatrixTest::quoted, (field, value) -> value);

        assertEquals("(\"score\" IS NULL AND \"id\" > ?)", predicate.sql());
        assertEquals(java.util.List.of(7L), predicate.parameters());
    }

    @Test
    void nullsFirstIncludesNonNullRowsAndThenContinuesInsideTheNullGroup() {
        DynamicForm form = form();
        KeysetPageNormalizer.NormalizedKeysetPage page = KeysetPageNormalizer.normalize(
                form,
                KeysetPageQuery.after(
                        20,
                        CursorPosition.of(Arrays.asList(null, 7L)),
                        KeysetSort.asc("score", NullOrder.FIRST)));

        SqlFragment predicate = KeysetPredicateRenderer.render(
                page, KeysetNullableMatrixTest::quoted, (field, value) -> value);

        assertEquals("(\"score\" IS NOT NULL OR (\"score\" IS NULL AND \"id\" > ?))",
                     predicate.sql());
        assertEquals(java.util.List.of(7L), predicate.parameters());
    }

    @Test
    void aNonNullableTieBreakerCannotCarryANullPosition() {
        assertThrows(IllegalArgumentException.class,
                     () -> KeysetPageNormalizer.normalize(
                             form(),
                             KeysetPageQuery.after(
                                     20,
                                     CursorPosition.of(Arrays.asList(5, null)),
                                     KeysetSort.asc("score", NullOrder.LAST))));
    }

    private static DynamicForm form() {
        return DynamicForm.builder("scores", "scores")
                .addField(DynamicField.of("score", "INTEGER"))
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .build();
    }

    private static String quoted(String field) {
        return '"' + field + '"';
    }
}
