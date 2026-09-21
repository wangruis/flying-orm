package com.flying.orm.rdb.operator;

import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Function;

/**
 * 动态删除的响应式链式门面。逻辑删除、物理删除、Scope 和乐观锁先统一编译成 {@link WriteSpec}，
 * 本类只负责选择响应式执行方法。
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public final class DmlDeleteOperator {

    private final ReactiveFormClient formClient;
    private final DmlWriteCommand command;
    private boolean physical;

    DmlDeleteOperator(ReactiveFormClient formClient, SqlRenderer renderer, String table) {
        this.formClient = Objects.requireNonNull(formClient, "form client must not be null");
        this.command = DmlWriteCommand.delete(renderer, table);
    }

    /** 设置严格业务条件；没有有效条件时底层仍会阻止意外全表删除。 */
    public DmlDeleteOperator where(Function<WhereDsl, WhereDsl> customizer) {
        command.where(customizer);
        return this;
    }

    /** 显式开启删除乐观锁。 */
    public DmlDeleteOperator optimisticLock(OptimisticLockOptions lock) {
        command.optimisticLock(lock);
        return this;
    }

    /** 使用常见 0/1 逻辑删除约定。 */
    public DmlDeleteOperator logicDelete(String fieldName) {
        return logicDelete(fieldName, 0, 1);
    }

    /** 声明逻辑删除字段及业务值。 */
    public DmlDeleteOperator logicDelete(String fieldName, Object notDeletedValue, Object deletedValue) {
        command.logicDelete(fieldName, notDeletedValue, deletedValue);
        return this;
    }

    /** 显式切换为物理 DELETE；Scope 和乐观锁保护不会关闭。 */
    public DmlDeleteOperator physical() {
        this.physical = true;
        return this;
    }

    /** 追加本次数据范围，只会继续收紧默认保护。 */
    public DmlDeleteOperator scope(DataScope scope) {
        command.scope(scope);
        return this;
    }

    /** 使用客户端默认执行保护提交删除。 */
    public Mono<Long> execute() {
        WriteSpec spec = command.spec();
        return physical ? formClient.physicalDelete(spec) : formClient.delete(spec);
    }

    /** 使用本次显式执行保护提交删除。 */
    public Mono<Long> execute(SqlExecutionOptions options) {
        WriteSpec spec = command.spec().withExecutionOptions(options);
        return physical ? formClient.physicalDelete(spec) : formClient.delete(spec);
    }
}
