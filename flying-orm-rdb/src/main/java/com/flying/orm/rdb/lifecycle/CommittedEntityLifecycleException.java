package com.flying.orm.rdb.lifecycle;

import com.flying.orm.core.error.OrmErrorReport;
import com.flying.orm.core.error.OrmErrorReportProvider;

import java.util.Objects;

/**
 * 数据库写入已经提交，但实体 POST 回调执行失败时抛出的确定性异常。
 *
 * <p>调用方不得把该异常当作“数据库未执行”而盲目重试；{@link #committed()} 永远返回 {@code true}，
 * {@link #result()} 保留已提交操作的行数或批次结果，便于上层进入补偿、告警或幂等确认流程。</p>
 *
 * @author wangr
 * @date 2026-08-04
 * @version v1.0
 */
public final class CommittedEntityLifecycleException extends RuntimeException
        implements OrmErrorReportProvider {

    private static final long serialVersionUID = 1L;

    private final EntityLifecyclePhase phase;
    private final Object result;

    /**
     * 创建提交后回调异常，只接受写入完成后的三个 POST 阶段。
     *
     * @param phase 失败的提交后阶段
     * @param result 已提交数据库操作的结果
     * @param cause 原始回调异常
     */
    public CommittedEntityLifecycleException(EntityLifecyclePhase phase, Object result, Throwable cause) {
        super("database change committed but entity lifecycle callback failed at "
                      + requirePostWritePhase(phase).name(),
              Objects.requireNonNull(cause, "entity lifecycle failure cause must not be null"));
        this.phase = phase;
        this.result = result;
    }

    /** @return 固定为 true，明确数据库事实已经提交。 */
    public boolean committed() {
        return true;
    }

    /** @return 失败的提交后生命周期阶段。 */
    public EntityLifecyclePhase phase() {
        return phase;
    }

    /** @return 已提交操作的行数、批次结果或其他结果对象。 */
    public Object result() {
        return result;
    }

    /** @return 可供上层稳定识别和记录的生命周期错误报告。 */
    @Override
    public OrmErrorReport toErrorReport() {
        return new OrmErrorReport("LIFECYCLE", "POST_COMMIT_CALLBACK_FAILED", phase.name(), null, null,
                                  getMessage());
    }

    private static EntityLifecyclePhase requirePostWritePhase(EntityLifecyclePhase phase) {
        EntityLifecyclePhase safePhase = Objects.requireNonNull(phase, "entity lifecycle phase must not be null");
        return switch (safePhase) {
            case POST_PERSIST, POST_UPDATE, POST_REMOVE -> safePhase;
            default -> throw new IllegalArgumentException(
                    "committed lifecycle exception requires a post-write phase: " + safePhase);
        };
    }
}
