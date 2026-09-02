package com.flying.orm.rdb.transaction;

import java.util.Objects;

/**
 * 外部事务参与时 ORM 无法安全完成某项动作的稳定错误。
 *
 * <p>调用方可以按 {@link Reason} 判断是需要换成 ATOMIC，还是只需要等待外部事务管理器给出最终提交结果。
 * 这类异常不携带 SQL 或参数，适合直接用于上层错误分类。</p>
 * @author wangr
 * @version v1.0
 */
public final class R2dbcTransactionParticipationException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    private final Reason reason;

    /** 创建带稳定原因的事务参与异常。 */
    public R2dbcTransactionParticipationException(Reason reason) {
        super(Objects.requireNonNull(reason, "transaction participation reason must not be null").message());
        this.reason = reason;
    }

    /** 返回调用方可稳定处理的拒绝或待确认原因。 */
    public Reason reason() {
        return reason;
    }

    /** 外部事务参与的固定边界。 */
    public enum Reason {
        /** INDEPENDENT 需要自行提交每个分片，不能嵌入外部事务。 */
        INDEPENDENT_BATCH_NOT_ALLOWED("independent batch cannot run inside an external transaction"),
        /** 回执重放需要在业务写入前额外读连接，本阶段不允许它绕开绑定连接。 */
        RECEIPT_RECOVERY_NOT_ALLOWED("receipt recovery cannot run inside an external transaction"),
        /** SQL 执行阶段失败或超时，最终提交权仍在外部，ORM 无法确认这一批的落库结果。 */
        OUTCOME_CONTROLLED_BY_EXTERNAL_TRANSACTION(
                "batch outcome is controlled by the external transaction and is not committed yet"),
        /** 外层事务结束后仍没有得到可靠的提交或回滚结论。 */
        EXTERNAL_TRANSACTION_OUTCOME_UNKNOWN("external transaction outcome remains unknown after completion");

        private final String message;

        Reason(String message) {
            this.message = message;
        }

        String message() {
            return message;
        }
    }
}
