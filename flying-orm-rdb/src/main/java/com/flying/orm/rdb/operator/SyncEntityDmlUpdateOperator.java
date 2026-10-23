package com.flying.orm.rdb.operator;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.form.SyncFormClient;

import java.util.Objects;

/**
 * 实体 Lambda 更新的同步命令。
 *
 * <p>它把共享 {@link EntityUpdateCommand} 生成的 WriteSpec 直接交给同步表单客户端，
 * 不会把 JDBC 调用塞进 Reactor。</p>
 *
 * @param <T> 实体类型
 * @author wangr
 * @version v2.0.0
 */
public final class SyncEntityDmlUpdateOperator<T> extends EntityUpdateCommand<T, SyncEntityDmlUpdateOperator<T>> {

    private final SyncFormClient client;

    SyncEntityDmlUpdateOperator(SyncFormClient client, EntityDmlModel<T> model, SqlRenderer renderer) {
        super(model, renderer, client.entityModels());
        this.client = Objects.requireNonNull(client, "form client must not be null");
    }

    @Override
    protected SyncEntityDmlUpdateOperator<T> self() {
        return this;
    }

    /** 使用客户端默认执行选项。 */
    public long execute() {
        return client.update(spec());
    }

    /** 使用本次显式执行选项。 */
    public long execute(SqlExecutionOptions options) {
        return client.update(spec().withExecutionOptions(options));
    }

}
