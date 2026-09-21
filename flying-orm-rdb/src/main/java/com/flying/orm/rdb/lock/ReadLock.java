package com.flying.orm.rdb.lock;

import java.util.Objects;

/**
 * 一次锁定读取的强度和等待方式，不接受任意 hint 字符串。
 *
 * @author wangr
 * @version v3.2
 */
public record ReadLock(ReadLockStrength strength, ReadLockWait waitMode) {

    public ReadLock {
        strength = Objects.requireNonNull(strength, "read lock strength must not be null");
        waitMode = Objects.requireNonNull(waitMode, "read lock wait mode must not be null");
    }

    public static ReadLock update() {
        return new ReadLock(ReadLockStrength.UPDATE, ReadLockWait.WAIT);
    }

    public static ReadLock updateNowait() {
        return new ReadLock(ReadLockStrength.UPDATE, ReadLockWait.NOWAIT);
    }

    public static ReadLock updateSkipLocked() {
        return new ReadLock(ReadLockStrength.UPDATE, ReadLockWait.SKIP_LOCKED);
    }
}
