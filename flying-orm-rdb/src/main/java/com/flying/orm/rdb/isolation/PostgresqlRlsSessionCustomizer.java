package com.flying.orm.rdb.isolation;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * PostgreSQL schema 与原生 RLS 会话变量实现。
 *
 * <p>schema 和 RLS 值都通过 {@code set_config} 参数绑定，不拼进 SQL。只有 reset 的配置名必须出现在 SQL 中，
 * 但 {@link IsolationContext} 已把它限制为点分标识符。设置使用 session 级作用域，因为普通响应式查询未必显式
 * 开事务；连接归还池前会逐项 reset，避免下一位租户继承本次会话值。</p>
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class PostgresqlRlsSessionCustomizer implements R2dbcSessionCustomizer {

    @Override
    public Mono<Void> initialize(Connection connection, IsolationContext context) {
        Mono<Void> setup = Mono.empty();
        if (context.schema() != null) {
            setup = executeValue(connection,
                                 "select set_config('search_path', quote_ident($1), false)",
                                 context.schema());
        }
        for (Map.Entry<String, String> setting : context.rlsSettings().entrySet()) {
            setup = setup.then(executeSetting(connection, setting.getKey(), setting.getValue()));
        }
        return setup;
    }

    @Override
    public Mono<Void> reset(Connection connection, IsolationContext context) {
        List<Mono<Void>> cleanup = new ArrayList<>();
        for (String setting : context.rlsSettings().keySet()) {
            cleanup.add(executeCommand(connection, "reset " + setting));
        }
        if (context.schema() != null) {
            cleanup.add(executeCommand(connection, "reset search_path"));
        }
        // 某一个 reset 失败也继续尝试后面的清理，最后再把错误交给连接池管理层处理。
        return Flux.fromIterable(cleanup).concatMapDelayError(action -> action).then();
    }

    private static Mono<Void> executeSetting(Connection connection, String name, String value) {
        Statement statement = connection.createStatement("select set_config($1, $2, false)")
                                        .bind(0, name)
                                        .bind(1, value);
        return consume(statement);
    }

    private static Mono<Void> executeValue(Connection connection, String sql, String value) {
        return consume(connection.createStatement(sql).bind(0, value));
    }

    private static Mono<Void> executeCommand(Connection connection, String sql) {
        return Flux.from(connection.createStatement(sql).execute())
                   .flatMap(Result::getRowsUpdated)
                   .then();
    }

    private static Mono<Void> consume(Statement statement) {
        // set_config 是 SELECT，必须消费返回行后才能在部分驱动上继续使用同一连接。
        return Flux.from(statement.execute())
                   .flatMap(result -> result.map((row, metadata) -> Boolean.TRUE))
                   .then();
    }
}
