package com.flying.orm.rdb.operator;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.spec.WriteSpec;

import java.util.Objects;

/**
 * 实体 Lambda 删除的同步命令。
 *
 * <p>默认逻辑删除与显式物理删除使用同一份条件、Scope 和乐观锁计划；区别只在最后调用同步客户端的
 * delete 还是 physicalDelete。这样物理删除不会变成绕过数据范围的特殊通道。</p>
 *
 * @param <T> 实体类型
 * @author wangr
 * @version v2.0.0
 */
public final class SyncEntityDmlDeleteOperator<T> extends EntityDeleteCommand<T, SyncEntityDmlDeleteOperator<T>> {

    private final SyncFormClient client;

    SyncEntityDmlDeleteOperator(SyncFormClient client, EntityDmlModel<T> model, SqlRenderer renderer) {
        super(model, renderer);
        this.client = Objects.requireNonNull(client, "form client must not be null");
    }

    @Override
    protected SyncEntityDmlDeleteOperator<T> self() {
        return this;
    }

    /** 使用客户端默认执行选项。 */
    public long execute() {
        return write(spec());
    }

    /** 使用本次显式执行选项。 */
    public long execute(SqlExecutionOptions options) {
        return write(spec().withExecutionOptions(options));
    }

    private long write(WriteSpec spec) {
        return physicalDelete() ? client.physicalDelete(spec) : client.delete(spec);
    }

}
