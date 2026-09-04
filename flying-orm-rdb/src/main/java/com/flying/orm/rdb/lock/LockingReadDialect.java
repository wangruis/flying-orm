package com.flying.orm.rdb.lock;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * 锁定读取在一个已确认数据库版本上的受控 SQL 形状。
 *
 * <p>这里不接受 hint 或 SQL 字符串。后缀型数据库只发布固定 {@code FOR UPDATE} 组合；SQL Server
 * 只发布固定表 hint。自定义旧方言默认 unsupported，能力未知时在 SQL 执行和连接获取前失败。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class LockingReadDialect {

    private static final LockingReadDialect UNSUPPORTED =
            new LockingReadDialect(Placement.UNSUPPORTED, Set.of(), false);
    private static final LockingReadDialect FOR_UPDATE =
            new LockingReadDialect(Placement.SUFFIX, EnumSet.allOf(ReadLockWait.class), true);
    private static final LockingReadDialect FOR_UPDATE_WITHOUT_PAGINATION =
            new LockingReadDialect(Placement.SUFFIX, EnumSet.allOf(ReadLockWait.class), false);
    private static final LockingReadDialect SQL_SERVER =
            new LockingReadDialect(Placement.TABLE_HINT, EnumSet.allOf(ReadLockWait.class), true);

    private final Placement placement;
    private final Set<ReadLockWait> waits;
    private final boolean pagination;

    private LockingReadDialect(Placement placement, Set<ReadLockWait> waits, boolean pagination) {
        this.placement = Objects.requireNonNull(placement, "locking read placement must not be null");
        this.waits = Set.copyOf(Objects.requireNonNull(waits, "locking read waits must not be null"));
        this.pagination = pagination;
    }

    /** @return 未认证方言共用的 fail-closed 实例 */
    public static LockingReadDialect unsupported() {
        return UNSUPPORTED;
    }

    /** @return 使用 FOR UPDATE 后缀的已认证实例 */
    public static LockingReadDialect forUpdateSuffix() {
        return FOR_UPDATE;
    }

    /**
     * @return 支持普通 {@code FOR UPDATE}，但数据库不允许把它和当前分页语法组合的实例
     */
    public static LockingReadDialect forUpdateSuffixWithoutPagination() {
        return FOR_UPDATE_WITHOUT_PAGINATION;
    }

    /** @return 使用固定 SQL Server 表 hint 的已认证实例 */
    public static LockingReadDialect sqlServerTableHint() {
        return SQL_SERVER;
    }

    /** 判断这个精确组合是否已经认证。 */
    public boolean supports(ReadLock lock) {
        ReadLock safeLock = Objects.requireNonNull(lock, "read lock must not be null");
        return safeLock.strength() == ReadLockStrength.UPDATE && waits.contains(safeLock.waitMode());
    }

    /** 判断这个锁是否还能与当前方言的分页语法安全组合。 */
    public boolean supportsPagination(ReadLock lock) {
        return pagination && supports(lock);
    }

    /**
     * 返回 SELECT 尾部后缀。表 hint 型方言返回空串；unsupported 会直接失败。
     */
    public String suffix(ReadLock lock) {
        ReadLock safeLock = requireSupported(lock);
        if (placement == Placement.TABLE_HINT) {
            return "";
        }
        return switch (safeLock.waitMode()) {
            case WAIT -> " FOR UPDATE";
            case NOWAIT -> " FOR UPDATE NOWAIT";
            case SKIP_LOCKED -> " FOR UPDATE SKIP LOCKED";
        };
    }

    /**
     * 返回紧随单表 FROM 标识符之后的固定 hint。后缀型方言返回空串。
     */
    public String tableHint(ReadLock lock) {
        ReadLock safeLock = requireSupported(lock);
        if (placement == Placement.SUFFIX) {
            return "";
        }
        return switch (safeLock.waitMode()) {
            case WAIT -> " WITH (UPDLOCK, ROWLOCK)";
            case NOWAIT -> " WITH (UPDLOCK, ROWLOCK, NOWAIT)";
            case SKIP_LOCKED -> " WITH (UPDLOCK, ROWLOCK, READPAST)";
        };
    }

    private ReadLock requireSupported(ReadLock lock) {
        ReadLock safeLock = Objects.requireNonNull(lock, "read lock must not be null");
        if (!supports(safeLock)) {
            throw new UnsupportedOperationException(
                    "locking read is not supported by this database descriptor");
        }
        return safeLock;
    }

    private enum Placement {
        SUFFIX,
        TABLE_HINT,
        UNSUPPORTED
    }
}
