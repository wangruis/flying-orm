package com.flying.orm.rdb.operator;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.spec.WriteSpec;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 以实体 Lambda 选择条件字段的响应式删除命令。
 *
 * <p>默认删除遵守实体的逻辑删除定义。物理删除也必须经过同一份 where、Scope 和乐观锁计划，
 * 不能因为调用了 {@link #physical()} 就绕过 fail-closed 的写入保护。</p>
 *
 * @param <T> 实体类型
 * @author wangr
 * @version v2.0.0
 */
public final class EntityDmlDeleteOperator<T> extends EntityDeleteCommand<T, EntityDmlDeleteOperator<T>> {

    private final ReactiveFormClient client;

    EntityDmlDeleteOperator(ReactiveFormClient client, EntityDmlModel<T> model, SqlRenderer renderer) {
        super(model, renderer);
        this.client = Objects.requireNonNull(client, "form client must not be null");
    }

    @Override
    protected EntityDmlDeleteOperator<T> self() {
        return this;
    }

    /** 使用客户端默认执行选项。 */
    public Mono<Long> execute() {
        return write(spec());
    }

    /** 使用本次显式执行选项。 */
    public Mono<Long> execute(SqlExecutionOptions options) {
        return write(spec().withExecutionOptions(options));
    }

    private Mono<Long> write(WriteSpec spec) {
        return physicalDelete() ? client.physicalDelete(spec) : client.delete(spec);
    }

}
