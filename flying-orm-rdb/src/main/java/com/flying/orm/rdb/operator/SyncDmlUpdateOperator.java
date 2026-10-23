package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.SyncFormClient;

import java.util.Objects;

/**
 * 同步动态更新门面。原生模式和响应式模式共用 {@link DmlWriteCommand} 的写入规格。
 *
 * @author wangr
 * @version v2.0.0
 */
public final class SyncDmlUpdateOperator extends DmlWriteCommand.Update<SyncDmlUpdateOperator> {

    private final SyncFormClient formClient;

    SyncDmlUpdateOperator(SyncFormClient formClient, SqlRenderer renderer, String table, DynamicForm form) {
        super(renderer, table, form);
        this.formClient = Objects.requireNonNull(formClient, "form client must not be null");
    }

    @Override
    protected SyncDmlUpdateOperator self() { return this; }

    /** 使用默认执行保护提交更新。 */
    public long execute() {
        return formClient.update(spec());
    }

    /** 使用本次显式执行保护提交更新。 */
    public long execute(SqlExecutionOptions options) {
        return formClient.update(spec().withExecutionOptions(options));
    }
}
