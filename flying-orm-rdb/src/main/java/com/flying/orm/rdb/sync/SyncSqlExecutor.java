package com.flying.orm.rdb.sync;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.jdbc.JdbcSqlExecutor;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Consumer;
import java.util.function.Predicate;
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
    default List<DynamicRow> query(SqlRequest request) {
        return queryMapped(request, null, row -> row, 0);
    }

    /**
     * 按顺序消费查询行并返回成功消费数；不收集结果列表。
     * options 为 null 时沿用执行器默认配置，回调期间不要重入同一连接。
     */
    default long queryEach(SqlRequest request, SqlExecutionOptions options, Consumer<? super DynamicRow> consumer) {
        Consumer<? super DynamicRow> safeConsumer = Objects.requireNonNull(consumer, "row consumer must not be null");
        return queryWhile(request, options, row -> { safeConsumer.accept(row); return true; });
    }

    /**
     * 同步查询的逐行执行端口。实现负责执行选项和资源清理，不得预先收集整批结果。
     * 回调返回 false 时停止读取，返回数包含该行；回调失败立即终止并传播原始异常。
     * options 为 null 时沿用执行器默认配置，回调期间不要重入同一连接。
     */
    long queryWhile(SqlRequest request, SqlExecutionOptions options, Predicate<? super DynamicRow> consumer);

    /** 逐行映射并消费，不收集列表；JDBC 在同一结果消费中绑定列布局。 */
    @InternalApi
    default <T> long queryWhile(SqlRequest request, SqlExecutionOptions options,
                                RowMapper<T> mapper, Predicate<? super T> consumer) {
        RowMapper<T> safeMapper = Objects.requireNonNull(mapper, "row mapper must not be null");
        Predicate<? super T> safeConsumer = Objects.requireNonNull(consumer, "row consumer must not be null");
        return queryWhile(request, options, row -> safeConsumer.test(safeMapper.map(row)));
    }

    /** 使用显式执行选项收集查询结果；容量规则由逐行执行端口负责。 */
    default List<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
        return queryMapped(request, Objects.requireNonNull(options, "sql execution options must not be null"),
                row -> row, 0);
    }

    /**
     * 逐行映射后只收集目标结果，不建立整批原始行列表。
     * JDBC 可直接绑定结果布局；此默认实现服务自定义逐行执行器。
     *
     * @param request SQL 请求
     * @param options 显式执行选项；null 表示沿用执行器默认配置
     * @param mapper 行映射器
     * @param rowLimit 终端最多消费的行数；0 表示全部
     * @param <T> 结果类型
     * @return 有序、只读的映射结果，允许映射结果为 null
     */
    @InternalApi
    default <T> List<T> queryMapped(SqlRequest request, SqlExecutionOptions options,
                                    RowMapper<T> mapper, int rowLimit) {
        if (rowLimit < 0) {
            throw new IllegalArgumentException("mapped query row limit must not be negative");
        }
        RowMapper<T> safeMapper = Objects.requireNonNull(mapper, "row mapper must not be null");
        List<T> mapped = new ArrayList<>();
        queryWhile(request, options, safeMapper, row -> {
            mapped.add(row);
            return rowLimit == 0 || mapped.size() < rowLimit;
        });
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
    default SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
        return rowsUpdatedReturningKeys(request, options, null);
    }

    /**
     * 执行写入并读取生成键；所有生成键入口由这一方法实现。
     *
     * @param request SQL 请求
     * @param options 执行保护选项
     * @param generatedKeyColumn 生成键物理列名；null 表示由驱动返回全部生成键
     * @return 写入结果
     */
    default SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request,
                                                    SqlExecutionOptions options,
                                                    String generatedKeyColumn) {
        throw new UnsupportedOperationException("sync sql executor does not support generated keys");
    }

    /** ORM 内部工作单元，在同一条上层连接上完成受保护字段业务写入和辅助关系维护。 */
    @InternalApi
    default SqlWriteResult protectedWrite(ProtectedWriteWork work, SqlExecutionOptions options) {
        Objects.requireNonNull(work, "protected write work must not be null");
        Objects.requireNonNull(options, "sql execution options must not be null");
        throw new UnsupportedOperationException("sync sql executor does not support protected writes");
    }


}
