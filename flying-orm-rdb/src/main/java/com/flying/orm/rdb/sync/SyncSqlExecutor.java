package com.flying.orm.rdb.sync;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlResultMemoryLimitExceededException;
import com.flying.orm.rdb.execution.SqlRowLimitExceededException;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.jdbc.JdbcSqlExecutor;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.observation.SqlStatementType;
import com.flying.orm.rdb.result.DynamicRow;

import java.util.List;
import java.util.Objects;
import javax.sql.DataSource;

/**
 * 同步 SQL 执行契约。V2 的同步内核直接使用原生 JDBC，和响应式 R2DBC 内核各自管理自己的连接与执行生命周期。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public interface SyncSqlExecutor {

    /**
     * 返回元数据缓存需要隔离的当前物理路由。普通单数据源和不知道路由的自定义执行器保持 {@code null}。
     *
     * <p>这是 ORM 内部协作契约，不参与稳定业务 API；动态 JDBC 执行器必须按调用返回当前路由，避免同名表
     * 在不同物理库之间共用元数据。</p>
     */
    @InternalApi
    default String metadataCachePartition() {
        return null;
    }

    /**
     * 从 DataSource 创建真正的 JDBC 同步执行器。该入口不经过 Reactor 或 R2DBC。
     *
     * @param dataSource 上层管理的 JDBC 数据源
     * @return 可并发共享的原生 JDBC 执行器
     */
    static SyncSqlExecutor jdbc(DataSource dataSource) {
        return JdbcSqlExecutor.create(dataSource);
    }

    /**
     * 执行查询 SQL。
     *
     * @param request SQL 请求
     * @return 查询结果
     */
    List<DynamicRow> query(SqlRequest request);

    /**
     * 带执行保护的同步查询。默认实现会在基础查询返回后检查最大行数和累计估算字节；
     * 原生 JDBC 实现还会把超时交给驱动，并在结果物化过程中提前执行相同预算。
     *
     * @param request SQL 请求
     * @param options 执行保护选项
     * @return 查询结果
     */
    default List<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options,
                                                                 "sql execution options must not be null");
        List<DynamicRow> rows = query(safeRequest);
        long estimatedBytes = 0L;
        SqlStatementType statementType = SqlStatementType.fromSql(safeRequest.sql());
        for (int index = 0; index < rows.size(); index++) {
            if (safeOptions.maxRows() > 0 && index >= safeOptions.maxRows()) {
                throw new SqlRowLimitExceededException(statementType, safeOptions.maxRows(), index);
            }
            estimatedBytes = saturatedAdd(estimatedBytes, BatchMemoryBudget.estimateRowBytes(rows.get(index)));
            if (safeOptions.maxResultBytes() > 0
                    && (estimatedBytes == Long.MAX_VALUE || estimatedBytes > safeOptions.maxResultBytes())) {
                throw new SqlResultMemoryLimitExceededException(
                        statementType, safeOptions.maxResultBytes(), estimatedBytes, index);
            }
        }
        return rows;
    }

    /**
     * 执行写入 SQL。
     *
     * @param request SQL 请求
     * @return 影响行数
     */
    long rowsUpdated(SqlRequest request);

    /**
     * 带执行保护的同步写入。具体 JDBC 实现负责把执行保护转换为驱动支持的 Statement 设置。
     *
     * @param request SQL 请求
     * @param options 执行保护选项
     * @return 影响行数
     */
    default long rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
        Objects.requireNonNull(options, "sql execution options must not be null");
        return rowsUpdated(request);
    }

    /** 执行写入并读取数据库生成键。实现必须在同一个 Statement 上取得影响行数和生成键。 */
    SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options);

    /**
     * ORM 内部按已校验的物理列名读取数据库生成键；不支持列名选择的自定义执行器继续使用原有入口。
     *
     * @param request SQL 请求
     * @param options 执行保护选项
     * @param generatedKeyColumn 已校验的生成键物理列名
     * @return 写入结果
     */
    @InternalApi
    default SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request,
                                                    SqlExecutionOptions options,
                                                    String generatedKeyColumn) {
        Objects.requireNonNull(generatedKeyColumn, "generated key column must not be null");
        return rowsUpdatedReturningKeys(request, options);
    }

    /** ORM 内部受保护字段写工作单元；只有能够控制同一连接事务的原生执行器可以覆盖。 */
    default SqlWriteResult atomicProtectedWrite(ProtectedWriteWork work, SqlExecutionOptions options) {
        Objects.requireNonNull(work, "protected write work must not be null");
        Objects.requireNonNull(options, "sql execution options must not be null");
        throw new UnsupportedOperationException("sync sql executor does not support atomic protected writes");
    }

    private static long saturatedAdd(long left, long right) {
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }

}
