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
 * 动态更新的响应式链式门面。命令规划由执行方式无关的 {@link DmlWriteCommand} 负责，本类只把最终
 * {@link WriteSpec} 交给真正非阻塞的 FormClient。
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public final class DmlUpdateOperator {

    private final ReactiveFormClient formClient;
    private final DmlWriteCommand command;

    DmlUpdateOperator(ReactiveFormClient formClient, SqlRenderer renderer, String table) {
        this.formClient = Objects.requireNonNull(formClient, "form client must not be null");
        this.command = DmlWriteCommand.update(renderer, table);
    }

    /** 设置一个待更新字段；值会继续进入统一 codec 和参数绑定。 */
    public DmlUpdateOperator set(String field, Object value) {
        command.set(field, value);
        return this;
    }

    /** 设置严格业务条件；可选搜索项应显式使用 {@code *IfPresent}。 */
    public DmlUpdateOperator where(Function<WhereDsl, WhereDsl> customizer) {
        command.where(customizer);
        return this;
    }

    /** 显式开启乐观锁。 */
    public DmlUpdateOperator optimisticLock(OptimisticLockOptions lock) {
        command.optimisticLock(lock);
        return this;
    }

    /** 使用常见 0/1 逻辑删除约定排除已删除数据。 */
    public DmlUpdateOperator logicDelete(String fieldName) {
        return logicDelete(fieldName, 0, 1);
    }

    /** 声明逻辑删除字段及业务值。 */
    public DmlUpdateOperator logicDelete(String fieldName, Object notDeletedValue, Object deletedValue) {
        command.logicDelete(fieldName, notDeletedValue, deletedValue);
        return this;
    }

    /** 追加本次数据范围，只会继续收紧默认保护。 */
    public DmlUpdateOperator scope(DataScope scope) {
        command.scope(scope);
        return this;
    }

    /** 使用客户端默认执行保护提交更新。 */
    public Mono<Long> execute() {
        return formClient.update(command.spec());
    }

    /** 使用本次显式执行保护提交更新。 */
    public Mono<Long> execute(SqlExecutionOptions options) {
        return formClient.update(command.spec().withExecutionOptions(options));
    }
}
