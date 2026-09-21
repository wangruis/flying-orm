package com.flying.orm.rdb.operator;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 原生 SQL 的同步入口。
 *
 * <p>原生 JDBC 构造路径直接调用 {@link SyncSqlExecutor}，不会创建 Reactor Publisher，也不会等待 R2DBC。
 * 同步与响应式入口只共享 SQL 规则和映射计划，不共享数据库执行线程或连接。</p>
 *
 * @author wangr
 * @date 2026-08-02
 * @version v1.0
 */
public final class SyncNativeSqlOperator {

    private final SyncSqlExecutor executor;

    private final EntityModelRegistry entityModels;

    private final ValueCodecRegistry valueCodecs;

    private final NativeSqlExecutionState state;

    /**
     * 为原生 JDBC 同步链路创建调用构建器。
     *
     * <p>构造器保持包内可见，统一由同步 Operator 门面装配，避免业务代码绕开统一的方言、codec
     * 和执行保护配置。</p>
     */
    SyncNativeSqlOperator(SyncSqlExecutor executor,
                          ValueCodecRegistry valueCodecs,
                          EntityModelRegistry entityModels,
                          RdbDialect dialect,
                          String sql) {
        this.executor = Objects.requireNonNull(executor, "sync SQL executor must not be null");
        this.valueCodecs = Objects.requireNonNull(valueCodecs, "value codec registry must not be null");
        this.entityModels = Objects.requireNonNull(entityModels, "entity model registry must not be null");
        this.state = new NativeSqlExecutionState(this.valueCodecs, dialect, sql, true);
    }

    /** @see NativeSqlOperator#bind(String, Object) */
    public SyncNativeSqlOperator bind(String name, Object value) {
        state.bind(name, value);
        return this;
    }

    /** @see NativeSqlOperator#bindNull(String, Class) */
    public SyncNativeSqlOperator bindNull(String name, Class<?> javaType) {
        state.bindNull(name, javaType);
        return this;
    }

    /** @see NativeSqlOperator#bindAll(Map) */
    public SyncNativeSqlOperator bindAll(Map<String, ?> values) {
        state.bindAll(values);
        return this;
    }

    /** @see NativeSqlOperator#options(SqlExecutionOptions) */
    public SyncNativeSqlOperator options(SqlExecutionOptions options) {
        state.options(options);
        return this;
    }

    /** 执行查询并等待完整 Map 列表。 */
    public List<DynamicRow> query() {
        return SyncSqlResultOperations.query(executor, state.request(), state.options());
    }

    /** 执行查询并等待完整类型化列表。 */
    public <T> List<T> query(Class<T> type) {
        return SyncSqlResultOperations.queryMapped(
                executor, state.request(), state.options(), entityModels.rawRowMapper(type, valueCodecs), 0);
    }

    /** 执行查询并使用自定义映射器等待完整列表。 */
    public <T> List<T> query(RowMapper<T> mapper) {
        return SyncSqlResultOperations.queryMapped(executor, state.request(), state.options(), mapper, 0);
    }

    /** 查询最多一行，空结果返回 null，多行结果明确报错。 */
    public DynamicRow one() {
        return SyncSqlResultOperations.one(executor, state.request(), state.options(), "native SQL one()");
    }

    /** 查询并映射最多一行，空结果返回 null。 */
    public <T> T one(Class<T> type) {
        return SyncSqlResultOperations.one(
                executor, state.request(), state.options(), entityModels.rawRowMapper(type, valueCodecs),
                "native SQL one()");
    }

    /** 使用自定义映射器查询最多一行，空结果返回 null。 */
    public <T> T one(RowMapper<T> mapper) {
        return SyncSqlResultOperations.one(
                executor, state.request(), state.options(), mapper, "native SQL one()");
    }

    /** 执行不返回普通行集的 SQL 并等待影响行数。 */
    public long execute() {
        return SyncSqlResultOperations.execute(executor, state.request(), state.options());
    }

    /** update 语义的易读别名。 */
    public long update() {
        return execute();
    }

    /** insert 语义的易读别名。 */
    public long insert() {
        return execute();
    }

    /** delete 语义的易读别名。 */
    public long delete() {
        return execute();
    }
}
