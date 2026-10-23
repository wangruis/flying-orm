package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FormFieldNameReuseTest {

    private final DynamicForm form = DynamicForm.builder("users", "users")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("userName", "VARCHAR")).build();

    @Test
    void canonicalProjectionReusesTheImmutableQueryList() {
        QuerySpec spec = query(List.of("id", "userName"));
        assertSame(spec.projections(), FormQueryShapeGuard.readableProjections(spec, form));
    }

    @Test
    void normalizationPreservesOrderDuplicatesAndUnreadableFieldRejection() {
        QuerySpec spec = query(List.of(" USERNAME ", "Id", "username"));
        List<String> normalized = FormQueryShapeGuard.readableProjections(spec, form);
        assertEquals(List.of("userName", "id", "userName"), normalized);
        assertEquals(List.of(" USERNAME ", "Id", "username"), spec.projections());
        assertThrows(UnsupportedOperationException.class, () -> normalized.set(0, "id"));
        DynamicForm readable = DynamicForm.builder("users", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        assertThrows(IllegalArgumentException.class,
                () -> FormQueryShapeGuard.readableProjections(query(List.of("id", "userName")), readable));
    }

    @Test
    void rendererCanonicalizationDoesNotRetainMutableListsOrTheirReadOnlyViews() {
        for (List<String> source : List.of(new ArrayList<>(List.of("id", "userName")),
                new LinkedList<>(List.of("id", "userName")),
                new LinkedList<>(List.of("id", " USERNAME ")))) {
            List<String> direct = FormSqlRenderSupport.canonicalFieldNames(form, source);
            List<String> view = FormSqlRenderSupport.canonicalFieldNames(form, Collections.unmodifiableList(source));
            source.clear();
            assertEquals(List.of("id", "userName"), direct);
            assertEquals(direct, view);
            assertThrows(UnsupportedOperationException.class, () -> direct.add("id"));
        }
    }

    @Test
    void rendererCanonicalizationStillValidatesCanonicalLookingNames() {
        assertThrows(IllegalArgumentException.class,
                () -> FormSqlRenderSupport.canonicalFieldNames(form, List.of("id", "missing")));
        assertThrows(IllegalArgumentException.class,
                () -> FormSqlRenderSupport.canonicalFieldNames(form, List.of("id", "userName;drop")));
        assertSame(List.of(), FormSqlRenderSupport.canonicalFieldNames(form, List.of()));
    }

    private QuerySpec query(List<String> projections) {
        return QuerySpec.of(form, ConditionGroup.and().build()).withProjection(projections, List.of());
    }

    @Test
    void emptyGroupsAndSortsReuseTheEmptyImmutableList() {
        QuerySpec spec = query(List.of("id"));
        assertSame(List.of(), FormQueryShapeGuard.readableGroups(spec, form));
        assertThrows(NullPointerException.class, () -> FormQueryShapeGuard.readableGroups(spec, null));
        List<PageSort> mutable = new ArrayList<>();
        List<PageSort> sorts = FormQueryShapeGuard.readableSorts(form, form, mutable);
        assertSame(List.of(), sorts);
        mutable.add(PageSort.asc("id"));
        assertEquals(List.of(), sorts);
    }

    @Test
    void canonicalSortsReuseTheirImmutableValuesWithoutRetainingTheInputList() {
        PageSort canonical = PageSort.desc("userName");
        List<PageSort> input = new ArrayList<>(List.of(canonical, PageSort.asc("ID"), canonical));
        List<PageSort> normalized = FormQueryShapeGuard.readableSorts(form, form, input);
        assertSame(canonical, normalized.getFirst());
        assertSame(canonical, normalized.get(2));
        assertEquals(List.of(canonical, PageSort.asc("id"), canonical), normalized);
        input.clear();
        assertEquals(3, normalized.size());
        assertThrows(UnsupportedOperationException.class, () -> normalized.add(canonical));
    }

    @Test
    void canonicalSortReuseDoesNotBypassReadabilityOrEncryptionChecks() {
        DynamicForm readable = DynamicForm.builder("users", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        assertThrows(IllegalArgumentException.class,
                () -> FormQueryShapeGuard.readableSorts(form, readable, List.of(PageSort.asc("userName"))));
        DynamicForm encrypted = DynamicForm.builder("users", "users")
                .addField(DynamicField.of("userName", "VARCHAR"))
                .encrypted("userName", EncryptedFieldDefinition.builder().build()).build();
        assertEquals("encrypted field must not be used for query ordering",
                assertThrows(IllegalArgumentException.class, () -> FormQueryShapeGuard.readableSorts(
                        encrypted, encrypted, List.of(PageSort.asc("userName")))).getMessage());
    }
}
