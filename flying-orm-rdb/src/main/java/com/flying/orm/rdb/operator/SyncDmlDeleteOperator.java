package com.flying.orm.rdb.operator;

import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.lock.OptimisticLockOptions;

import java.util.Objects;
import java.util.function.Function;

/**
 * 同步动态删除门面，直接把共享写入命令交给原生 JDBC 表单客户端。
 *
 * @author wangr
 * @version v2.0.0
 */
public final class SyncDmlDeleteOperator {

    private final SyncFormClient formClient;
    private final DmlWriteCommand command;
    private boolean physical;

    /** 原生 JDBC 构造器。 */
    SyncDmlDeleteOperator(SyncFormClient formClient, SqlRenderer renderer, String table) {
        this.formClient = Objects.requireNonNull(formClient, "sync form client must not be null");
        this.command = DmlWriteCommand.delete(renderer, table);
    }

    public SyncDmlDeleteOperator where(Function<WhereDsl, WhereDsl> customizer) {
        command.where(customizer);
        return this;
    }

    public SyncDmlDeleteOperator optimisticLock(OptimisticLockOptions lock) {
        command.optimisticLock(lock);
        return this;
    }

    public SyncDmlDeleteOperator logicDelete(String fieldName) {
        return logicDelete(fieldName, 0, 1);
    }

    public SyncDmlDeleteOperator logicDelete(String fieldName, Object notDeletedValue, Object deletedValue) {
        command.logicDelete(fieldName, notDeletedValue, deletedValue);
        return this;
    }

    /** 显式使用物理 DELETE，仍保留 Scope 与乐观锁保护。 */
    public SyncDmlDeleteOperator physical() {
        physical = true;
        return this;
    }

    public SyncDmlDeleteOperator scope(DataScope scope) {
        command.scope(scope);
        return this;
    }

    /** 使用默认执行保护提交删除。 */
    public long execute() {
        WriteSpec spec = command.spec();
        return physical ? formClient.physicalDelete(spec) : formClient.delete(spec);
    }

    /** 使用本次显式执行保护提交删除。 */
    public long execute(SqlExecutionOptions options) {
        WriteSpec spec = command.spec().withExecutionOptions(options);
        return physical ? formClient.physicalDelete(spec) : formClient.delete(spec);
    }
}
