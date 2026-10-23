package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.spec.WriteSpec;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 动态更新的响应式链式门面。命令规划由执行方式无关的 {@link DmlWriteCommand} 负责，本类只把最终
 * {@link WriteSpec} 交给真正非阻塞的 FormClient。
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public final class DmlUpdateOperator extends DmlWriteCommand.Update<DmlUpdateOperator> {

    private final ReactiveFormClient formClient;

    DmlUpdateOperator(ReactiveFormClient formClient, SqlRenderer renderer, String table, DynamicForm form) {
        super(renderer, table, form);
        this.formClient = Objects.requireNonNull(formClient, "form client must not be null");
    }

    @Override
    protected DmlUpdateOperator self() { return this; }

    /** 使用客户端默认执行保护提交更新。 */
    public Mono<Long> execute() {
        return formClient.update(spec());
    }

    /** 使用本次显式执行保护提交更新。 */
    public Mono<Long> execute(SqlExecutionOptions options) {
        return formClient.update(spec().withExecutionOptions(options));
    }
}
