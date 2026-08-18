package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.exception.RdbExceptionTranslator;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlTransactionSource;

import java.util.Objects;

/**
 * 把一组 JDBC 受保护写事务记录为一个稳定 SQL 执行事件。
 *
 * <p>业务写、owner 读取和侧索引维护仍由同一事务执行；观测只公开参数化业务 SQL，二进制密文和令牌继续由
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
    private SqlTransactionSource transactionSource = SqlTransactionSource.AUTO_COMMIT;

    JdbcProtectedWriteObservation(JdbcExecutionObservationSupport observations, ProtectedWriteWork work) {
        this.observations = Objects.requireNonNull(observations, "jdbc observations must not be null");
        this.work = Objects.requireNonNull(work, "protected write work must not be null");
    }

    /** 保存实际连接所有者，避免把外部事务误记成自动提交。 */
    void transactionSource(SqlTransactionSource source) {
        transactionSource = Objects.requireNonNull(source, "sql transaction source must not be null");
    }

    /** 发布已经完成事务清理的成功结果。 */
    void success(SqlWriteResult result) {
        SqlWriteResult safeResult = Objects.requireNonNull(result, "protected write result must not be null");
        observations.success(SqlExecutionOperation.UPDATE, work.writeRequest(), safeResult.affectedRows(),
                             startedAt, transactionSource);
    }

    /** 翻译并发布普通失败；嵌套 VME 仍按 JDBC 统一规则原样提升。 */
    RuntimeException failure(Throwable error) {
        VirtualMachineError fatal = JdbcThrowableGraph.findVirtualMachineError(error);
        if (fatal != null) {
            throw fatal;
        }
        RuntimeException translated = RdbExceptionTranslator.translate(error);
        long affectedRows = error instanceof GeneratedKeyReadException keyFailure
                ? keyFailure.affectedRows() : 0L;
        observations.failure(SqlExecutionOperation.UPDATE, work.writeRequest(), affectedRows, startedAt,
                             translated, transactionSource);
        return translated;
    }
}
