package com.flying.orm.rdb.form;

import com.flying.orm.core.page.CursorSort;
import com.flying.orm.core.page.PageSort;
import org.junit.jupiter.api.Test;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.RandomAccess;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FormQueryShapeGuardMembershipScanTest {

    private static final int WIDTH = 64;

    @Test
    void groupedProjectionAndSortMembershipReadsCanonicalGroupsLinearly() {
        List<String> fields = fields();
        CountingList groups = new CountingList(
                fields.stream().map(field -> field.toLowerCase(Locale.ROOT)).toList());
        List<String> projections = fields.stream()
                                         .map(field -> field.toUpperCase(Locale.ROOT))
                                         .toList();
        List<PageSort> sorts = fields.stream().map(PageSort::asc).toList();

        assertDoesNotThrow(() -> FormQueryShapeGuard.requireValidGrouping(projections, groups, sorts));

        assertLinearReads(groups, "group membership must not rescan canonical fields for every projection and sort");
    }

    @Test
    void cursorProjectionMembershipReadsCanonicalProjectionsLinearly() {
        List<String> fields = fields();
        CountingList projections = new CountingList(
                fields.stream().map(field -> field.toUpperCase(Locale.ROOT)).toList());
        List<CursorSort> sorts = fields.stream().map(CursorSort::asc).toList();

        assertDoesNotThrow(() -> FormQueryShapeGuard.requireCursorProjection(projections, sorts));

        assertLinearReads(projections,
                "cursor projection membership must not rescan canonical fields for every cursor sort");
    }

    @Test
    void preservesCaseInsensitiveMembershipForGroupingAndCursorProjection() {
        assertDoesNotThrow(() -> FormQueryShapeGuard.requireValidGrouping(
                List.of("ACCOUNT_ID"), List.of("account_id"), List.of(PageSort.asc("AcCoUnT_Id"))));
        assertDoesNotThrow(() -> FormQueryShapeGuard.requireCursorProjection(
                List.of("ACCOUNT_ID"), List.of(CursorSort.asc("AcCoUnT_Id"))));
    }

    @Test
    void preservesUnicodeEqualsIgnoreCaseMembership() {
        String alias = "F\u0130ELD";

        assertTrue("field".equalsIgnoreCase(alias));
        assertDoesNotThrow(() -> FormQueryShapeGuard.requireValidGrouping(
                List.of("field"), List.of(alias), List.of(PageSort.asc(alias))));
        assertDoesNotThrow(() -> FormQueryShapeGuard.requireCursorProjection(
                List.of("field"), List.of(CursorSort.asc(alias))));
    }

    @Test
    void groupedProjectionFailureStillPrecedesSortFailure() {
        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> FormQueryShapeGuard.requireValidGrouping(
                        List.of("missing_projection"),
                        List.of("group_field"),
                        List.of(PageSort.asc("missing_sort"))));

        assertEquals("grouped query projection must be a grouping field", failure.getMessage());
    }

    @Test
    void emptyGroupsAndProjectionsKeepTheirMemberReadFastPaths() {
        CountingList groups = new CountingList(List.of());
        CountingList projections = new CountingList(List.of());

        assertDoesNotThrow(() -> FormQueryShapeGuard.requireValidGrouping(
                List.of("not_a_group"), groups, List.of(PageSort.asc("also_not_a_group"))));
        assertDoesNotThrow(() -> FormQueryShapeGuard.requireCursorProjection(
                projections, List.of(CursorSort.asc("not_a_projection"))));

        assertEquals(0, groups.memberReads(), "empty groups must return before reading members");
        assertEquals(0, projections.memberReads(), "empty projections must return before reading members");
    }

    private static List<String> fields() {
        List<String> fields = new ArrayList<>(WIDTH);
        for (int index = 0; index < WIDTH; index++) {
            fields.add("field_" + index);
        }
        return fields;
    }

    private static void assertLinearReads(CountingList fields, String message) {
        assertTrue(fields.memberReads() <= WIDTH * 2L,
                () -> message + ": expected at most " + (WIDTH * 2L)
                        + " member reads but was " + fields.memberReads());
    }

    private static final class CountingList extends AbstractList<String> implements RandomAccess {
        private final List<String> source;
        private long memberReads;

        private CountingList(List<String> source) {
            this.source = source;
        }

        @Override
        public String get(int index) {
            memberReads++;
            return source.get(index);
        }

        @Override
        public int size() {
            return source.size();
        }

        private long memberReads() {
            return memberReads;
        }
    }
}
