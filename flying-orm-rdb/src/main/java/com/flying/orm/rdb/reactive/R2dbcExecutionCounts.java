package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.LongConsumer;
import java.util.stream.LongStream;

/**
 * R2DBC 执行路径中已确认计数的精确汇总。
 *
 * <p>影响行数、回执行数和输入偏移都不能在超出 {@code long} 表示范围时回绕或饱和；
 * 两者都会把不可证实的数值伪装成确定结果。此类型只供 reactive 包内部协作。</p>
 *
 * @author wangr
 * @date 2026-08-08
 * @version v1.0
 */
final class R2dbcExecutionCounts {

    private R2dbcExecutionCounts() {
    }

    static long add(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            throw new RdbException(RdbErrorKind.UNKNOWN,
                                   "database execution count exceeds supported range",
                                   null,
                                   null,
                                   overflow);
        }
    }

    static long sum(LongStream values) {
        return values.reduce(0L, R2dbcExecutionCounts::add);
    }

    /** 在消费计数时发布事实；结果流随后取消或报错不能抹掉已经确认的行数。 */
    static Mono<Long> sum(Flux<Long> values, LongConsumer confirmedRows) {
        if (confirmedRows == null) {
            return values.reduce(0L, R2dbcExecutionCounts::add);
        }
        return values.reduce(0L, (left, right) -> {
            long total = add(left, right);
            confirmedRows.accept(total);
            return total;
        });
    }
}
