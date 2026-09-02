package com.flying.orm.rdb.batch;

import java.util.Objects;

/**
 * BatchResolution 表示一次 UNKNOWN 回执查询目前能确认到的结果。
 *
 * @param status 状态
 * @param token  被查询的恢复令牌
 * @author wangr
 * @date 2026-07-23
 * @version v1.0
 */
public record BatchResolution(Status status, BatchChunkResult.RecoveryToken token) {

    /**
     * 检查确认结果。
     */
    public BatchResolution {
        status = Objects.requireNonNull(status, "batch resolution status must not be null");
        token = Objects.requireNonNull(token, "batch resolution token must not be null");
    }

    /**
     * 创建已经找到匹配回执的结果。
     *
     * @param token 恢复令牌
     * @return 已提交结果
     */
    public static BatchResolution committed(BatchChunkResult.RecoveryToken token) {
        return new BatchResolution(Status.COMMITTED, token);
    }

    /**
     * 创建暂时仍无法确认的结果。
     *
     * @param token 恢复令牌
     * @return 未知结果
     */
    public static BatchResolution unknown(BatchChunkResult.RecoveryToken token) {
        return new BatchResolution(Status.UNKNOWN, token);
    }

    /** 回执确认状态。 */
    public enum Status {
        /** 已找到与恢复令牌一致的已提交回执。 */
        COMMITTED,
        /** 数据库不可用或回执暂时不可见。 */
        UNKNOWN
    }
}
