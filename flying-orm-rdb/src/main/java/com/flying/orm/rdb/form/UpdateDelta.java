package com.flying.orm.rdb.form;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * 表示 update 语句中的受限数值增减量。
 *
 * <p>该类型不是通用 SQL 表达式容器，只能由 {@link #increment(Number)} 或
 * {@link #decrement(Number)} 创建。SQL 渲染器仍会校验目标字段是数值类型，并把增减量作为参数绑定，
 * 因而不会形成绕过字段白名单或拼接任意 SQL 的入口。</p>
 *
 * @param value 带方向的增减量；正数表示增加，负数表示减少
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record UpdateDelta(Number value) {

    /**
     * 创建增加操作。
     *
     * @param amount 大于零的增加量
     * @return 受限增加量
     */
    public static UpdateDelta increment(Number amount) {
        return new UpdateDelta(requirePositive(amount, "increment amount"));
    }

    /**
     * 创建减少操作。
     *
     * @param amount 大于零的减少量
     * @return 受限减少量
     */
    public static UpdateDelta decrement(Number amount) {
        BigDecimal positive = requirePositive(amount, "decrement amount");
        return new UpdateDelta(positive.negate());
    }

    /** 校验直接构造时也不能注入空值、零、NaN 或无穷大。 */
    public UpdateDelta {
        value = requireNonZero(value);
    }

    private static BigDecimal requirePositive(Number value, String name) {
        BigDecimal decimal = decimal(value, name);
        if (decimal.signum() <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return decimal;
    }

    private static Number requireNonZero(Number value) {
        BigDecimal decimal = decimal(value, "update delta");
        if (decimal.signum() == 0) {
            throw new IllegalArgumentException("update delta must not be zero");
        }
        return decimal;
    }

    private static BigDecimal decimal(Number value, String name) {
        Number safeValue = Objects.requireNonNull(value, name + " must not be null");
        if (safeValue instanceof Double doubleValue && !Double.isFinite(doubleValue)
                || safeValue instanceof Float floatValue && !Float.isFinite(floatValue)) {
            throw new IllegalArgumentException(name + " must be finite");
        }
        try {
            return new BigDecimal(safeValue.toString());
        } catch (NumberFormatException error) {
            throw new IllegalArgumentException(name + " must be a decimal number", error);
        }
    }
}
