package com.flying.orm.core.scope;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.internal.condition.ConditionValueNormalizer;
import com.flying.orm.core.internal.condition.ConditionValuePolicy;
import com.flying.orm.core.internal.value.BindableValueSnapshots;

import java.util.Objects;

/**
 * 服务端时间范围。上层先算好明确边界，这里只负责校验并转换成参数化条件。
 *
 * @param field         时间字段
 * @param start         开始值，没有开始边界时为 null
 * @param startBoundary 开始边界类型，没有开始边界时为 null
 * @param end           结束值，没有结束边界时为 null
 * @param endBoundary   结束边界类型，没有结束边界时为 null
 * @author wangr
 * @date 2026-07-31
 * @version v1.0
 */
public record TimeScope(String field,
                        Object start,
                        Boundary startBoundary,
                        Object end,
                        Boundary endBoundary) {

    public TimeScope {
        field = requireText(field, "time field");
        requireMatchingBoundary(start, startBoundary, "start");
        requireMatchingBoundary(end, endBoundary, "end");
        if (start == null && end == null) {
            throw new IllegalArgumentException("time scope needs at least one boundary");
        }
        start = normalizeBoundary(start);
        end = normalizeBoundary(end);
        validateRange(start, startBoundary, end, endBoundary);
    }

    /** @return 与构造入参和上一次读取隔离的开始边界 */
    @Override
    public Object start() {
        return BindableValueSnapshots.immutableValue(start);
    }

    /** @return 与构造入参和上一次读取隔离的结束边界 */
    @Override
    public Object end() {
        return BindableValueSnapshots.immutableValue(end);
    }

    /**
     * 创建左闭右开的时间窗口，也就是 {@code start <= value < end}。
     */
    public static TimeScope between(String field, Object startInclusive, Object endExclusive) {
        return new TimeScope(field,
                             Objects.requireNonNull(startInclusive, "time start must not be null"),
                             Boundary.INCLUSIVE,
                             Objects.requireNonNull(endExclusive, "time end must not be null"),
                             Boundary.EXCLUSIVE);
    }

    /**
     * 创建两端都包含的时间窗口，也就是 {@code start <= value <= end}。
     */
    public static TimeScope closed(String field, Object startInclusive, Object endInclusive) {
        return new TimeScope(field,
                             Objects.requireNonNull(startInclusive, "time start must not be null"),
                             Boundary.INCLUSIVE,
                             Objects.requireNonNull(endInclusive, "time end must not be null"),
                             Boundary.INCLUSIVE);
    }

    /**
     * 创建只有开始边界的时间窗口，也就是 value >= start。
     */
    public static TimeScope from(String field, Object startInclusive) {
        return new TimeScope(field,
                             Objects.requireNonNull(startInclusive, "time start must not be null"),
                             Boundary.INCLUSIVE,
                             null,
                             null);
    }

    /**
     * 创建只有结束边界的时间窗口，也就是 {@code value < end}。
     */
    public static TimeScope before(String field, Object endExclusive) {
        return new TimeScope(field,
                             null,
                             null,
                             Objects.requireNonNull(endExclusive, "time end must not be null"),
                             Boundary.EXCLUSIVE);
    }

    /**
     * 把时间边界转换成普通条件，后续 Repository、Operator 和 FormClient 都走同一条渲染链路。
     */
    public ConditionGroup toCondition() {
        ConditionGroup.Builder builder = ConditionGroup.and();
        if (start != null) {
            builder.where(field, startBoundary == Boundary.INCLUSIVE ? ">=" : ">", start);
        }
        if (end != null) {
            builder.where(field, endBoundary == Boundary.INCLUSIVE ? "<=" : "<", end);
        }
        return builder.build();
    }

    /**
     * 边界是否包含传入的时间点。
     */
    public enum Boundary {
        INCLUSIVE,
        EXCLUSIVE
    }

    private static void requireMatchingBoundary(Object value, Boundary boundary, String name) {
        if ((value == null) != (boundary == null)) {
            throw new IllegalArgumentException("time " + name + " value and boundary must be provided together");
        }
    }

    private static void validateRange(Object start, Boundary startBoundary, Object end, Boundary endBoundary) {
        if (start == null || end == null) {
            return;
        }
        if (!start.getClass().equals(end.getClass()) || !(start instanceof Comparable<?> comparable)) {
            throw new IllegalArgumentException("time boundaries must use the same comparable type");
        }
        @SuppressWarnings("unchecked")
        int compared = ((Comparable<Object>) comparable).compareTo(end);
        boolean emptyAtSameValue = compared == 0
                && (startBoundary == Boundary.EXCLUSIVE || endBoundary == Boundary.EXCLUSIVE);
        if (compared > 0 || emptyAtSameValue) {
            throw new IllegalArgumentException("time start must be before time end");
        }
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.strip();
    }

    private static Object normalizeBoundary(Object value) {
        if (value == null) {
            return null;
        }
        if (value.getClass().isArray()) {
            throw new IllegalArgumentException("time boundary must not be an array");
        }
        Object normalized = ConditionValueNormalizer.normalize(ConditionValueShape.SCALAR,
                                                               value,
                                                               ConditionValuePolicy.REJECT_EMPTY)
                                                    .value();
        return BindableValueSnapshots.immutableValue(normalized);
    }
}
