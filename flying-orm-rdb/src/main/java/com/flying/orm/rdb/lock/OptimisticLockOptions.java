package com.flying.orm.rdb.lock;

import com.flying.orm.core.internal.value.BindableValueSnapshots;

import java.util.Objects;

/**
 * 一次 update/delete 要带的乐观锁信息。
 *
 * <p>底层表单客户端只认这份明确参数；Repository 可以根据实体上的 @Version 帮你生成它。</p>
 *
 * @param field         版本字段名
 * @param expectedValue 调用方读数据时拿到的旧版本值
 * @param nextValue     要写回的新版本值，只有 ASSIGN 模式会用
 * @param mode          新版本怎么写
 * @author wangr
 * @date 2026-07-30
 * @version v1.0
 */
public record OptimisticLockOptions(String field,
                                    Object expectedValue,
                                    Object nextValue,
                                    OptimisticLockMode mode) {

    public OptimisticLockOptions {
        field = requireText(field, "optimistic lock field");
        expectedValue = snapshot(Objects.requireNonNull(expectedValue,
                                                        "optimistic lock expected value must not be null"));
        mode = Objects.requireNonNull(mode, "optimistic lock mode must not be null");
        if (mode == OptimisticLockMode.ASSIGN) {
            nextValue = snapshot(Objects.requireNonNull(nextValue, "optimistic lock next value must not be null"));
        }
    }

    public static OptimisticLockOptions increment(String field, Object expectedValue) {
        return new OptimisticLockOptions(field, expectedValue, null, OptimisticLockMode.INCREMENT);
    }

    public static OptimisticLockOptions assign(String field, Object expectedValue, Object nextValue) {
        return new OptimisticLockOptions(field, expectedValue, nextValue, OptimisticLockMode.ASSIGN);
    }

    /**
     * 返回构造时固定的旧版本值；可变可绑定值每次都返回不可变快照，避免调用方改写后续 WHERE 参数。
     *
     * @return 构造时固定的旧版本值
     */
    @Override
    public Object expectedValue() {
        return snapshot(expectedValue);
    }

    /**
     * 返回构造时固定的新版本值；仅 ASSIGN 模式有值，可变可绑定值每次都返回不可变快照。
     *
     * @return 新版本值；INCREMENT 模式为 {@code null}
     */
    @Override
    public Object nextValue() {
        return snapshot(nextValue);
    }

    private static String requireText(String value, String name) {
        String safeValue = Objects.requireNonNull(value, name + " must not be null").trim();
        if (safeValue.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return safeValue;
    }

    private static Object snapshot(Object value) {
        return BindableValueSnapshots.immutableValue(value);
    }
}
