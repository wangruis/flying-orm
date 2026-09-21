package com.flying.orm.rdb.sync;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlResultMemoryLimitExceededException;
import com.flying.orm.rdb.execution.SqlRowLimitExceededException;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.jdbc.JdbcSqlExecutor;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.observation.SqlStatementType;
import com.flying.orm.rdb.result.DynamicRow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import com.flying.orm.rdb.jdbc.JdbcConnectionAccess;

/**
 * 同步 SQL 执行契约。原生 JDBC 执行 SQL 并释放所拥有的资源；外部事务和执行、清理时限由上层治理。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public interface SyncSqlExecutor {

    /** 在一条上层连接上执行多语句工作。自定义实现必须明确保证同连接。 */
    @InternalApi
    default <T> T withConnection(SqlRequest firstRequest, Function<SyncSqlExecutor, T> work) {
        Objects.requireNonNull(work, "connection work must not be null");
        throw new UnsupportedOperationException("sync executor does not support same-connection work");
    }

    /**
     * 从上层连接访问端口创建原生 JDBC 同步执行器。该入口不经过 Reactor 或 R2DBC。
     *
     * @param connectionAccess 上层管理的 JDBC 连接访问端口
     * @param dialect 上层显式选择的数据库方言
     * @return 可并发共享的原生 JDBC 执行器
     */
    static SyncSqlExecutor jdbc(JdbcConnectionAccess connectionAccess, RdbDialect dialect) {
        return JdbcSqlExecutor.create(connectionAccess, dialect);
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
     * 原生 JDBC 实现在结果物化过程中提前执行相同容量保护。
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
        if (safeOptions.maxRows() <= 0 && safeOptions.maxResultBytes() <= 0) {
            return rows;
        }
        long estimatedBytes = 0L;
        SqlStatementType statementType = SqlStatementType.fromSql(safeRequest.sql());
        int index = 0;
        for (DynamicRow row : rows) {
            if (safeOptions.maxRows() > 0 && index >= safeOptions.maxRows()) {
                throw new SqlRowLimitExceededException(statementType, safeOptions.maxRows(), index);
            }
            if (safeOptions.maxResultBytes() > 0) {
                estimatedBytes = saturatedAdd(estimatedBytes, BatchMemoryBudget.estimateRowBytes(row));
                if (estimatedBytes == Long.MAX_VALUE || estimatedBytes > safeOptions.maxResultBytes()) {
                    throw new SqlResultMemoryLimitExceededException(
                            statementType, safeOptions.maxResultBytes(), estimatedBytes, index);
                }
            }
            index++;
        }
        return rows;
    }

    /**
     * ORM 内部的类型化查询终端。{@code rowLimit=0} 表示读取全部行；正数只限制本次终端需要消费的行数，
     * 不替代调用方在 {@link SqlExecutionOptions} 中声明的结果保护。原生 JDBC 实现会在 ResultSet 生命周期内
     * 直接映射；自定义执行器继续复用原有查询契约并在结果返回后做兼容映射。
     *
     * @param request SQL 请求
     * @param options 显式执行保护；null 表示继续使用执行器默认保护
     * @param mapper 行映射器
     * @param rowLimit 终端最多需要消费的行数；0 表示全部
     * @param <T> 结果类型
     * @return 有序、只读的映射结果
     */
    @InternalApi
    default <T> List<T> queryMapped(SqlRequest request,
                                    SqlExecutionOptions options,
                                    RowMapper<T> mapper,
                                    int rowLimit) {
        if (rowLimit < 0) {
            throw new IllegalArgumentException("mapped query row limit must not be negative");
        }
        RowMapper<T> safeMapper = Objects.requireNonNull(mapper, "row mapper must not be null");
        List<DynamicRow> rows = Objects.requireNonNull(
                options == null ? query(request) : query(request, options),
                "sync SQL executor query must not return null");
        int mappedSize = rowLimit == 0 ? rows.size() : Math.min(rowLimit, rows.size());
        List<T> mapped = new ArrayList<>(mappedSize);
        Iterator<DynamicRow> iterator = rows.iterator();
        for (int index = 0; index < mappedSize; index++) {
            mapped.add(safeMapper.map(iterator.next()));
        }
        return Collections.unmodifiableList(mapped);
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

    /** 在同一条上层连接上完成受保护字段业务写入和辅助关系维护。 */
    default SqlWriteResult protectedWrite(ProtectedWriteWork work, SqlExecutionOptions options) {
        Objects.requireNonNull(work, "protected write work must not be null");
        Objects.requireNonNull(options, "sql execution options must not be null");
        throw new UnsupportedOperationException("sync sql executor does not support protected writes");
    }

    private static long saturatedAdd(long left, long right) {
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }

}
