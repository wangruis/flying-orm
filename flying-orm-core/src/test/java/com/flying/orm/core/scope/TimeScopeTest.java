package com.flying.orm.core.scope;

import com.flying.orm.core.condition.TermCondition;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证服务端时间窗口会生成明确的边界条件，并在 SQL 生成前拒绝无效区间。
 *
 * @author wangr
 * @date 2026-07-31
 * @version v1.0
 */
class TimeScopeTest {

    @Test
    void buildsHalfOpenClosedAndSingleBoundaryConditions() {
        LocalDateTime start = LocalDateTime.of(2026, 7, 1, 0, 0);
        LocalDateTime end = LocalDateTime.of(2026, 8, 1, 0, 0);

        assertTerms(TimeScope.between("created_at", start, end), List.of(">=", "<"), List.of(start, end));
        assertTerms(TimeScope.closed("created_at", start, end), List.of(">=", "<="), List.of(start, end));
        assertTerms(TimeScope.from("created_at", start), List.of(">="), List.of(start));
        assertTerms(TimeScope.before("created_at", end), List.of("<"), List.of(end));
    }

    @Test
    void rejectsEmptyReversedAndIncompatibleWindows() {
        LocalDateTime start = LocalDateTime.of(2026, 7, 1, 0, 0);
        LocalDateTime end = LocalDateTime.of(2026, 8, 1, 0, 0);

        assertThrows(IllegalArgumentException.class, () -> TimeScope.between("created_at", start, start));
        assertThrows(IllegalArgumentException.class, () -> TimeScope.closed("created_at", end, start));
        assertThrows(IllegalArgumentException.class, () -> TimeScope.between("created_at", start, end.toLocalDate()));
    }

    /** 验证时间边界始终是可比较的时间标量，不能因条件标量数组支持而被放宽。 */
    @Test
    void rejectsArrayTimeBoundariesWithStableMessage() {
        IllegalArgumentException from = assertThrows(IllegalArgumentException.class,
                                                     () -> TimeScope.from("created_at", new byte[]{1}));
        IllegalArgumentException before = assertThrows(IllegalArgumentException.class,
                                                       () -> TimeScope.before("created_at", new Object[]{"x"}));
        IllegalArgumentException between = assertThrows(IllegalArgumentException.class,
                                                        () -> TimeScope.between("created_at",
                                                                                new byte[]{1},
                                                                                new byte[]{2}));
        IllegalArgumentException closed = assertThrows(IllegalArgumentException.class,
                                                       () -> TimeScope.closed("created_at",
                                                                              new Object[]{"x"},
                                                                              new Object[]{"y"}));

        assertEquals("time boundary must not be an array", from.getMessage());
        assertEquals("time boundary must not be an array", before.getMessage());
        assertEquals("time boundary must not be an array", between.getMessage());
        assertEquals("time boundary must not be an array", closed.getMessage());
    }

    private static void assertTerms(TimeScope scope, List<String> operators, List<?> values) {
        List<TermCondition> terms = scope.toCondition()
                                         .children()
                                         .stream()
                                         .map(TermCondition.class::cast)
                                         .toList();
        assertEquals(operators, terms.stream().map(TermCondition::operator).toList());
        assertEquals(values, terms.stream().map(TermCondition::value).toList());
        assertEquals(List.of("created_at"), terms.stream().map(TermCondition::field).distinct().toList());
    }
}
