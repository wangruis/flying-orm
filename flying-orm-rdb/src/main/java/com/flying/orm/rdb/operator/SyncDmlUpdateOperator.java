package com.flying.orm.rdb.operator;

import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.lock.OptimisticLockOptions;

import java.util.Objects;
import java.util.function.Function;

/**
 * 同步动态更新门面。原生模式和响应式模式共用 {@link DmlWriteCommand} 的写入规格。
 *
 * @author wangr
 * @version v2.0.0
 */
public final class SyncDmlUpdateOperator {

    private final SyncFormClient formClient;
    private final DmlWriteCommand command;

    /** 原生 JDBC 构造器。 */
    SyncDmlUpdateOperator(SyncFormClient formClient, SqlRenderer renderer, String table) {
        this.formClient = Objects.requireNonNull(formClient, "sync form client must not be null");
        this.command = DmlWriteCommand.update(renderer, table);
    }

    public SyncDmlUpdateOperator set(String field, Object value) {
        command.set(field, value);
        return this;
    }

    public SyncDmlUpdateOperator where(Function<WhereDsl, WhereDsl> customizer) {
        command.where(customizer);
        return this;
    }

    public SyncDmlUpdateOperator optimisticLock(OptimisticLockOptions lock) {
        command.optimisticLock(lock);
        return this;
    }

    public SyncDmlUpdateOperator logicDelete(String fieldName) {
        return logicDelete(fieldName, 0, 1);
    }

    public SyncDmlUpdateOperator logicDelete(String fieldName, Object notDeletedValue, Object deletedValue) {
        command.logicDelete(fieldName, notDeletedValue, deletedValue);
        return this;
    }

    public SyncDmlUpdateOperator scope(DataScope scope) {
        command.scope(scope);
        return this;
    }

    /** 使用默认执行保护提交更新。 */
    public long execute() {
        return formClient.update(command.spec());
    }

    /** 使用本次显式执行保护提交更新。 */
    public long execute(SqlExecutionOptions options) {
        return formClient.update(command.spec().withExecutionOptions(options));
    }
}
