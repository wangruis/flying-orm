package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.exception.RdbExceptionTranslator;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.observation.SqlExecutionOperation;

import java.util.Objects;

/**
 * 把一组 JDBC 受保护写操作记录为一个稳定 SQL 执行事件。
 *
 * <p>业务写、owner 读取和侧索引维护使用同一连接；观测只公开参数化业务 SQL，二进制密文和令牌继续由
 * 统一日志脱敏器处理。致命 JVM 错误保持原样传播，不让观测改变资源清理语义。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class JdbcProtectedWriteObservation {

    private final JdbcExecutionObservationSupport observations;
    private final ProtectedWriteWork work;
    private final long startedAt = System.nanoTime();
    private long confirmedRows;


    JdbcProtectedWriteObservation(JdbcExecutionObservationSupport observations, ProtectedWriteWork work) {
        this.observations = Objects.requireNonNull(observations, "jdbc observations must not be null");
        this.work = Objects.requireNonNull(work, "protected write work must not be null");
    }


    /** 主写返回后立即保留实际行数，不等待侧索引或连接释放完成。 */
    void confirmedRows(long rows) {
        confirmedRows = rows;
    }

    /** 发布已经完成语句资源清理的成功结果。 */
    void success(SqlWriteResult result) {
        SqlWriteResult safeResult = Objects.requireNonNull(result, "protected write result must not be null");
        observations.success(SqlExecutionOperation.UPDATE, work.writeRequest(), safeResult.affectedRows(),
                             startedAt);
    }

    /** 翻译并发布普通失败；异常图解释只由统一 translator 执行一次。 */
    RuntimeException failure(Throwable error) {
        RuntimeException translated = RdbExceptionTranslator.translate(error);
        long affectedRows = error instanceof GeneratedKeyReadException keyFailure
                ? Math.max(confirmedRows, keyFailure.affectedRows()) : confirmedRows;
        observations.failure(SqlExecutionOperation.UPDATE, work.writeRequest(), affectedRows, startedAt,
                             translated);
        return translated;
    }
}
