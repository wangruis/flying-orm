package com.flying.orm.rdb.operator;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 以实体 Lambda 选择字段的响应式更新命令。
 *
 * <p>字段可写性、原子增减和乐观锁校验不在这里复制实现，而是交给 {@link EntityUpdateCommand}。
 * 这样响应式执行和原生 JDBC 同步执行使用一套 WriteSpec，参数顺序和安全规则始终一致。</p>
 *
 * @param <T> 实体类型
 * @author wangr
 * @version v2.0.0
 */
public final class EntityDmlUpdateOperator<T> extends EntityUpdateCommand<T, EntityDmlUpdateOperator<T>> {

    private final ReactiveFormClient client;

    EntityDmlUpdateOperator(ReactiveFormClient client, EntityDmlModel<T> model, SqlRenderer renderer) {
        super(model, renderer, client.entityModels());
        this.client = Objects.requireNonNull(client, "form client must not be null");
    }

    @Override
    protected EntityDmlUpdateOperator<T> self() {
        return this;
    }

    /** 使用客户端默认执行选项。 */
    public Mono<Long> execute() {
        return client.update(spec());
    }

    /** 使用本次显式执行选项。 */
    public Mono<Long> execute(SqlExecutionOptions options) {
        return client.update(spec().withExecutionOptions(options));
    }

}
