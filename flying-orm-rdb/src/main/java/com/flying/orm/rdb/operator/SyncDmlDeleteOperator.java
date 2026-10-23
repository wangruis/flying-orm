package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.spec.WriteSpec;

import java.util.Objects;

/**
 * 同步动态删除门面，直接把共享写入命令交给原生 JDBC 表单客户端。
 *
 * @author wangr
 * @version v2.0.0
 */
public final class SyncDmlDeleteOperator extends DmlWriteCommand.Delete<SyncDmlDeleteOperator> {

    private final SyncFormClient formClient;

    SyncDmlDeleteOperator(SyncFormClient formClient, SqlRenderer renderer, String table, DynamicForm form) {
        super(renderer, table, form);
        this.formClient = Objects.requireNonNull(formClient, "form client must not be null");
    }

    @Override
    protected SyncDmlDeleteOperator self() { return this; }

    /** 使用默认执行保护提交删除。 */
    public long execute() {
        WriteSpec spec = spec();
        return physicalDelete() ? formClient.physicalDelete(spec) : formClient.delete(spec);
    }

    /** 使用本次显式执行保护提交删除。 */
    public long execute(SqlExecutionOptions options) {
        WriteSpec spec = spec().withExecutionOptions(options);
        return physicalDelete() ? formClient.physicalDelete(spec) : formClient.delete(spec);
    }
}
