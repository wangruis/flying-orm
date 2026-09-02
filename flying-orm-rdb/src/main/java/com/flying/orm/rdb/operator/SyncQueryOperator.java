package com.flying.orm.rdb.operator;

import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * 动态查询的同步门面。
 *
 * <p>它和响应式 {@link QueryOperator} 共用 {@link DmlQueryCommand}，但最终只调用
 * {@link SyncSqlExecutor}，不会创建 Publisher 或等待 R2DBC。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public final class SyncQueryOperator {

    private final SyncSqlExecutor executor;
    private final DmlQueryCommand command;

    /** 原生 JDBC 构造器，只保存同步执行能力和共享命令状态。 */
    SyncQueryOperator(SyncSqlExecutor executor, SqlRenderer renderer, DataScope defaultDataScope) {
        this.executor = Objects.requireNonNull(executor, "sync sql executor must not be null");
        this.command = new DmlQueryCommand(renderer, defaultDataScope);
    }

    /** 追加查询字段。 */
    public SyncQueryOperator select(String... columns) {
        command.select(columns);
        return this;
    }

    /** 设置目标物理表。 */
    public SyncQueryOperator from(String table) {
        command.from(table);
        return this;
    }

    /** 设置结构化业务条件。 */
    public SyncQueryOperator where(Consumer<WhereDsl> consumer) {
        command.where(consumer);
        return this;
    }

    /** 使用 0/1 逻辑删除约定。 */
    public SyncQueryOperator logicDelete(String fieldName) {
        return logicDelete(fieldName, 0, 1);
    }

    /** 声明逻辑删除字段和业务值。 */
    public SyncQueryOperator logicDelete(String fieldName, Object notDeletedValue, Object deletedValue) {
        command.logicDelete(fieldName, notDeletedValue, deletedValue);
        return this;
    }

    /** 追加本次数据范围，不会覆盖默认范围。 */
    public SyncQueryOperator scope(DataScope scope) {
        command.scope(scope);
        return this;
    }

    /** 使用默认执行保护返回完整的有界结果列表。 */
    public List<DynamicRow> fetchMap() {
        return executor.query(command.toRequest());
    }

    /** 使用本次显式执行保护返回完整的有界结果列表。 */
    public List<DynamicRow> fetchMap(SqlExecutionOptions options) {
        return executor.query(command.toRequest(), options);
    }
}
