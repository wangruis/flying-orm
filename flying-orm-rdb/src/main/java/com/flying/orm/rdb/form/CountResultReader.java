package com.flying.orm.rdb.form;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Objects;

/**
 * 把数据库 COUNT 结果严格转换成分页模型使用的 long。
 *
 * <p>不同驱动可能返回 Long、BigInteger、BigDecimal 或自己的 Number 实现。直接调用 longValue() 会把
 * 超出范围的值截断，还会把小数和 NaN 悄悄改成另一个整数。分页总数宁可明确报错，也不能向业务层返回
 * 一个看起来正常、实际已经损坏的数字。</p>
 */
final class CountResultReader {

    private CountResultReader() {
    }

    static long read(Map<String, Object> row) {
        Map<String, Object> safeRow = Objects.requireNonNull(row, "count row must not be null");
        Object value = safeRow.get("total");
        if (value == null) {
            value = safeRow.get("TOTAL");
        }
        if (value == null && !safeRow.isEmpty()) {
            value = safeRow.values().iterator().next();
        }
        if (value instanceof Number number) {
            try {
                // 十进制文本保留 BigInteger 和驱动自定义 Number 的完整精度，longValueExact 同时检查小数和溢出。
                return new BigDecimal(number.toString()).longValueExact();
            } catch (ArithmeticException | NumberFormatException error) {
                throw new IllegalArgumentException("count result total must be an exact long integer", error);
            }
        }
        if (value instanceof CharSequence text) {
            try {
                return Long.parseLong(text.toString());
            } catch (NumberFormatException error) {
                throw new IllegalArgumentException("count result total must be an exact long integer", error);
            }
        }
        throw new IllegalArgumentException("count result total must be an exact long integer");
    }
}
