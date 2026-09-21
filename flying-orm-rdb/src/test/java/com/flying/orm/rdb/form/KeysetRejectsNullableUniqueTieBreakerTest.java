package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorDirection;
import com.flying.orm.core.page.CursorPosition;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.NullOrder;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KeysetRejectsNullableUniqueTieBreakerTest {

    @Test
    void appendsTheCompletePrimaryKeyAsHiddenStableTieBreaker() {
        DynamicForm form = DynamicForm.builder("orders", "orders")
                .addField(DynamicField.of("created_at", "TIMESTAMP"))
                .addField(DynamicField.primaryKey("tenant_id", "BIGINT"))
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .build();
        KeysetPageQuery page = KeysetPageQuery.first(
                20, KeysetSort.desc("created_at", NullOrder.LAST));

        KeysetPageNormalizer.NormalizedKeysetPage normalized =
                KeysetPageNormalizer.normalize(form, page);

        assertEquals(List.of("created_at", "tenant_id", "id"),
                     normalized.sorts().stream().map(KeysetSort::field).toList());
        assertEquals(List.of("tenant_id", "id"), normalized.hiddenTieBreakers());
        assertEquals(CursorDirection.DESC, normalized.sorts().get(2).direction());
        assertEquals(NullOrder.LAST, normalized.sorts().get(2).nullOrder());
    }

    @Test
    void rejectsNullableUniqueAndAcceptsOnlyANonNullUniqueFallback() {
        DynamicForm nullableUnique = DynamicForm.builder("users", "users")
                .addField(DynamicField.of("email", "VARCHAR").withUnique(true))
                .addField(DynamicField.of("name", "VARCHAR"))
                .build();
        KeysetPageQuery page = KeysetPageQuery.first(
                20, KeysetSort.asc("name", NullOrder.FIRST));

        assertThrows(IllegalArgumentException.class,
                     () -> KeysetPageNormalizer.normalize(nullableUnique, page));

        DynamicForm nonNullUnique = DynamicForm.builder("users", "users")
                .addField(DynamicField.of("email", "VARCHAR")
                                      .withNullable(false)
                                      .withUnique(true))
                .addField(DynamicField.of("name", "VARCHAR"))
                .build();
        assertEquals(List.of("email"),
                     KeysetPageNormalizer.normalize(nonNullUnique, page).hiddenTieBreakers());
    }

    @Test
    void validatesTheTypedPositionAgainstTheFinalStableSort() {
        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.of("occurred_at", "TIMESTAMP"))
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .build();
        KeysetPageQuery valid = KeysetPageQuery.after(
                20,
                CursorPosition.of(Arrays.asList(null, 7L)),
                KeysetSort.asc("occurred_at", NullOrder.LAST));

        assertEquals(2, KeysetPageNormalizer.normalize(form, valid).positionValues().size());

        KeysetPageQuery missingTieBreaker = KeysetPageQuery.after(
                20,
                CursorPosition.of(List.of(7L)),
                KeysetSort.asc("occurred_at", NullOrder.LAST));
        assertThrows(IllegalArgumentException.class,
                     () -> KeysetPageNormalizer.normalize(form, missingTieBreaker));
    }
}
