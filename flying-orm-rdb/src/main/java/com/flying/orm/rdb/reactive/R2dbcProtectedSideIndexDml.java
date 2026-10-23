package com.flying.orm.rdb.reactive;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

/** Executes a bounded, verified R2DBC statement batch for protected side-index tokens. */
final class R2dbcProtectedSideIndexDml {

    private R2dbcProtectedSideIndexDml() {
    }

    static Mono<Void> insertParameterSets(Connection connection, String sql, List<List<Object>> parameterSets) {
        return execute(connection, sql, parameterSets, true);
    }

    static Mono<Void> deleteParameterSets(Connection connection, String sql, List<List<Object>> parameterSets) {
        return execute(connection, sql, parameterSets, false);
    }

    private static Mono<Void> execute(Connection connection, String sql, List<List<Object>> parameterSets,
                                      boolean insert) {
        if (parameterSets.isEmpty()) return Mono.empty();
        return Mono.defer(() -> {
            Statement statement = connection.createStatement(sql);
            for (int index = 0; index < parameterSets.size(); index++) {
                bind(statement, parameterSets.get(index));
                if (index + 1 < parameterSets.size()) statement.add();
            }
            Flux<Long> counts = Flux.from(statement.execute()).concatMap(Result::getRowsUpdated, 1);
            return insert ? counts.reduce(0L, R2dbcExecutionCounts::add)
                    .flatMap(total -> total == parameterSets.size() ? Mono.empty()
                            : Mono.error(new IllegalStateException(
                                    "protected side index insert batch must affect one row per token")))
                    : counts.then();
        });
    }

    private static void bind(Statement statement, List<Object> parameters) {
        for (int index = 0; index < parameters.size(); index++) {
            Object value = parameters.get(index);
            if (value == null) {
                statement.bindNull(index, Object.class);
            } else {
                statement.bind(index, R2dbcParameterValues.forBinding(value));
            }
        }
    }
}
