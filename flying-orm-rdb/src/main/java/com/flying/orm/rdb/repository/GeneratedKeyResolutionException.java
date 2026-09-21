package com.flying.orm.rdb.repository;

import com.flying.orm.core.error.OrmErrorReport;
import com.flying.orm.core.error.OrmErrorReportProvider;

import java.util.Objects;

/**
 * 数据库写入已经执行，但数据库生成主键无法读取或应用到实体时抛出的异常。
 *
 * <p>只报告生成键读取或实体回填阶段及已知影响行数。异常消息不包含实体、主键值或驱动原文，
 * 调用方不得把它当作“SQL 未执行”而盲目重试。</p>
 *
 * @author wangr
 * @date 2026-08-17
 * @version v2.0
 */
public final class GeneratedKeyResolutionException extends RuntimeException
        implements OrmErrorReportProvider {

    private static final long serialVersionUID = 1L;

    private final long affectedRows;
    private final Phase phase;

    /**
     * 创建生成键解析失败异常。
     *
     * @param affectedRows 数据库报告或生成键行证明的影响行数
     * @param phase 生成键解析失败阶段
     * @param cause 原始生成键读取、转换或实体写回失败
     */
    public GeneratedKeyResolutionException(long affectedRows, Phase phase, Throwable cause) {
        super("generated primary key resolution failed at "
                      + Objects.requireNonNull(phase, "generated key resolution phase must not be null").name(),
              Objects.requireNonNull(cause, "generated key resolution failure cause must not be null"));
        if (affectedRows < 0L) {
            throw new IllegalArgumentException("generated key affected rows must not be negative");
        }
        this.affectedRows = affectedRows;
        this.phase = phase;
    }

    /** @return 数据库报告或生成键行证明的影响行数。 */
    public long affectedRows() {
        return affectedRows;
    }

    /** @return 生成键读取或实体回填的失败阶段。 */
    public Phase phase() {
        return phase;
    }

    /** @return 可供上层稳定识别和记录的脱敏错误报告。 */
    @Override
    public OrmErrorReport toErrorReport() {
        return new OrmErrorReport("REPOSITORY", "GENERATED_KEY_RESOLUTION_FAILED",
                                  phase.name(), null, null, getMessage());
    }

    /** 生成键解析的实际失败阶段。 */
    public enum Phase {
        READ,
        ASSIGN
    }
}
