package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.spec.WriteSpec;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 动态删除的响应式链式门面。逻辑删除、物理删除、Scope 和乐观锁先统一编译成 {@link WriteSpec}，
 * 本类只负责选择响应式执行方法。
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public final class DmlDeleteOperator extends DmlWriteCommand.Delete<DmlDeleteOperator> {

    private final ReactiveFormClient formClient;

    DmlDeleteOperator(ReactiveFormClient formClient, SqlRenderer renderer, String table, DynamicForm form) {
        super(renderer, table, form);
        this.formClient = Objects.requireNonNull(formClient, "form client must not be null");
    }

    @Override
    protected DmlDeleteOperator self() { return this; }

    /** 使用客户端默认执行保护提交删除。 */
    public Mono<Long> execute() {
        WriteSpec spec = spec();
        return physicalDelete() ? formClient.physicalDelete(spec) : formClient.delete(spec);
    }

    /** 使用本次显式执行保护提交删除。 */
    public Mono<Long> execute(SqlExecutionOptions options) {
        WriteSpec spec = spec().withExecutionOptions(options);
        return physicalDelete() ? formClient.physicalDelete(spec) : formClient.delete(spec);
    }
}
