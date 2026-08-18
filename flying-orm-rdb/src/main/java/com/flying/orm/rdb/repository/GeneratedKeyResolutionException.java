package com.flying.orm.rdb.repository;

import com.flying.orm.core.error.OrmErrorReport;
import com.flying.orm.core.error.OrmErrorReportProvider;

import java.util.Objects;

/**
 * 数据库写入已经执行，但数据库生成主键无法读取或应用到实体时抛出的确定状态异常。
 *
 * <p>执行器完整返回后实体回填失败为 {@link WriteState#COMMITTED}；参与外部事务时为
 * {@link WriteState#ENLISTED}；生成键结果仍在读取时失败且没有外部事务则为 {@link WriteState#UNKNOWN}。
 * 异常消息不包含实体、主键值或驱动原文，调用方不得把它当作“SQL 未执行”而盲目重试。</p>
 *
 * @author wangr
 * @date 2026-08-17
 * @version v2.0
 */
public final class GeneratedKeyResolutionException extends RuntimeException
        implements OrmErrorReportProvider {

    private static final long serialVersionUID = 1L;

    private final long affectedRows;
    private final WriteState state;

    /**
     * 创建生成键解析失败异常。
     *
     * @param affectedRows 数据库报告或生成键行证明的影响行数
     * @param state 数据库写入的可确认状态
     * @param cause 原始生成键读取、转换或实体写回失败
     */
    public GeneratedKeyResolutionException(long affectedRows, WriteState state, Throwable cause) {
        super(message(Objects.requireNonNull(state, "generated key write state must not be null")),
              Objects.requireNonNull(cause, "generated key resolution failure cause must not be null"));
        if (affectedRows < 0L) {
            throw new IllegalArgumentException("generated key affected rows must not be negative");
        }
        this.affectedRows = affectedRows;
        this.state = state;
    }

    /** @return 数据库报告或生成键行证明的影响行数。 */
    public long affectedRows() {
        return affectedRows;
    }

    /** @return 数据库事务是否已经确认提交。 */
    public boolean committed() {
        return state == WriteState.COMMITTED;
    }

    /** @return 写入是否只加入了仍由上层控制的外部事务。 */
    public boolean enlisted() {
        return state == WriteState.ENLISTED;
    }

    /** @return 写入最终结果是否仍然未知。 */
    public boolean unknown() {
        return state == WriteState.UNKNOWN;
    }

    /** @return 数据库写入的可确认状态。 */
    public WriteState state() {
        return state;
    }

    /** @return 可供上层稳定识别和记录的脱敏错误报告。 */
    @Override
    public OrmErrorReport toErrorReport() {
        return new OrmErrorReport("REPOSITORY", "GENERATED_KEY_RESOLUTION_FAILED",
                                  state.name(), null, null, getMessage());
    }

    private static String message(WriteState state) {
        return switch (state) {
            case COMMITTED -> "database write committed but generated primary key could not be resolved";
            case ENLISTED -> "database write enlisted but generated primary key could not be resolved";
            case UNKNOWN -> "database write outcome is unknown because generated primary key could not be read";
        };
    }

    /** 数据库写入在生成键解析失败时可以可靠表达的三种状态。 */
    public enum WriteState {
        COMMITTED,
        ENLISTED,
        UNKNOWN
    }
}
