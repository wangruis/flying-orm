package com.flying.orm.core.internal.condition;

import com.flying.orm.core.condition.ConditionValueException;
import com.flying.orm.core.condition.ConditionValueShape;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static com.flying.orm.core.condition.ConditionValueException.Error.RANGE_ORDER_INVALID;
import static com.flying.orm.core.condition.ConditionValueException.Error.RANGE_SIZE_INVALID;
import static com.flying.orm.core.internal.condition.ConditionValuePolicy.IGNORE_EMPTY;
import static com.flying.orm.core.internal.condition.ConditionValuePolicy.REJECT_EMPTY;
import static com.flying.orm.core.condition.ConditionValueShape.COLLECTION;
import static com.flying.orm.core.condition.ConditionValueShape.SCALAR_OR_COLLECTION;
import static com.flying.orm.core.condition.ConditionValueShape.NONE;
import static com.flying.orm.core.condition.ConditionValueShape.RANGE;
import static com.flying.orm.core.condition.ConditionValueShape.SCALAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证所有条件入口复用的值整理规则，不让空白和错误形状流到 SQL 层。
 *
 * @author wangr
 * @date 2026-07-31
 * @version v1.0
 */
class ConditionValueNormalizerTest {

    @Test
    void stripsScalarsAndCleansCollections() {
        assertEquals("张三", normalize(SCALAR, "  张三  ", REJECT_EMPTY).value());
        assertEquals(List.of("u1", "u2"),
                     normalize(COLLECTION,
                               Arrays.asList(" ", null, " u1 ", "u2"),
                               REJECT_EMPTY).value());
        assertFalse(normalize(SCALAR, "   ", IGNORE_EMPTY).present());
    }

    @Test
    void validatesNoneAndRangeShapes() {
        ConditionValueNormalizer.Result noValue = normalize(NONE, null, REJECT_EMPTY);

        assertTrue(noValue.present());
        assertNull(noValue.value());
        assertEquals(List.of(1, 2), normalize(RANGE, List.of(1, 2), REJECT_EMPTY).value());
        assertError(RANGE_SIZE_INVALID, () -> normalize(RANGE, List.of(1), REJECT_EMPTY));
        assertError(RANGE_ORDER_INVALID, () -> normalize(RANGE, List.of(2, 1), REJECT_EMPTY));
    }

    @Test
    void acceptsScalarAndCollectionOnlyForTheExplicitMixedShape() {
        assertEquals("org-1", normalize(SCALAR_OR_COLLECTION, " org-1 ", REJECT_EMPTY).value());
        assertEquals(List.of("org-1", "org-2"),
                     normalize(SCALAR_OR_COLLECTION,
                               List.of(" org-1 ", "org-2"),
                               REJECT_EMPTY).value());
    }

    @Test
    void rejectsCollectionsAboveDefaultSafetyLimit() {
        assertThrows(ConditionValueException.class,
                     () -> normalize(COLLECTION,
                                     java.util.stream.IntStream.rangeClosed(0, 1_000)
                                                               .boxed()
                                                               .toList(),
                                     REJECT_EMPTY));
    }

    private static ConditionValueNormalizer.Result normalize(ConditionValueShape shape,
                                                             Object value,
                                                             ConditionValuePolicy policy) {
        return ConditionValueNormalizer.normalize(shape, value, policy);
    }

    private static void assertError(ConditionValueException.Error error, Runnable action) {
        ConditionValueException exception = assertThrows(ConditionValueException.class, action::run);
        assertEquals(error, exception.error());
    }
}
