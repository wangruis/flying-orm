package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 给执行器补一份默认保护。调用方不传 options 时用默认值，显式传入时以本次配置为准。
 * 这是包内装饰器，统一从 {@link ReactiveSqlExecutor#withDefaultExecutionOptions(SqlExecutionOptions)} 进入。
 * 原生执行器在连接可用后实施 SQL timeout；只实现单参数方法的自定义执行器由接口默认逻辑保留结果容量保护，
 * 不会越权给可能包含连接池排队的整个 Publisher 计时。
 * 对象创建后字段不再变化，可以和底层执行器一样被多个请求并发复用。
 *
 * @author wangr
 * @date 2026-07-30
 * @version v1.0
 */
final class DefaultOptionsReactiveSqlExecutor extends ForwardingReactiveSqlExecutor {

    private final SqlExecutionOptions options;

    private DefaultOptionsReactiveSqlExecutor(ReactiveSqlExecutor delegate, SqlExecutionOptions options) {
        super(delegate);
        this.options = Objects.requireNonNull(options, "sql execution options must not be null");
    }

    static ReactiveSqlExecutor create(ReactiveSqlExecutor delegate, SqlExecutionOptions options) {
        ReactiveSqlExecutor safeDelegate = Objects.requireNonNull(delegate, "reactive sql executor must not be null");
        safeDelegate = ForwardingReactiveSqlExecutor.withoutPolicy(
                safeDelegate, DefaultOptionsReactiveSqlExecutor.class, ignored -> {
                });
        return ForwardingReactiveSqlExecutor.preservingScopedCapability(
                safeDelegate, new DefaultOptionsReactiveSqlExecutor(safeDelegate, options));
    }

    @Override
    ForwardingReactiveSqlExecutor redecoratePolicy(ReactiveSqlExecutor delegate) {
        return new DefaultOptionsReactiveSqlExecutor(delegate, options);
    }

    @Override
    public Flux<DynamicRow> query(SqlRequest request) {
        return delegate().query(request, options);
    }

    @Override
    public Flux<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
        return delegate().query(request, options);
    }

    @Override
    public Mono<Long> rowsUpdated(SqlRequest request) {
        return delegate().rowsUpdated(request, options);
    }

    @Override
    public Mono<Long> rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
        return delegate().rowsUpdated(request, options);
    }

}
