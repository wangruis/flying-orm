package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.internal.protection.ProtectedReplacementBatchPlan;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

/** Executes a bounded, verified R2DBC statement batch for protected side-index tokens. */
final class R2dbcProtectedSideIndexDml {

    static final int MAX_TOKEN_BATCH_SIZE = 500;

    private R2dbcProtectedSideIndexDml() {
    }

    static Mono<Void> insertTokens(Connection connection,
                                   String sql,
                                   ProtectedWriteWork work,
                                   Map<String, Object> owner,
                                   ProtectedWriteWork.FieldTokens field) {
        int tokenCount = field.tokenCount();
        if (tokenCount == 0) {
            return Mono.empty();
        }
        int chunks = (tokenCount + MAX_TOKEN_BATCH_SIZE - 1) / MAX_TOKEN_BATCH_SIZE;
        return Flux.range(0, chunks)
                   .concatMap(chunk -> executeChunk(
                           connection, sql, work, owner, field,
                           chunk * MAX_TOKEN_BATCH_SIZE,
                           Math.min(tokenCount, (chunk + 1) * MAX_TOKEN_BATCH_SIZE)), 1)
                   .then();
    }

    static Mono<Void> insertParameterSets(Connection connection,
                                          String sql,
                                          List<List<Object>> parameterSets) {
        if (parameterSets.size() > MAX_TOKEN_BATCH_SIZE) {
            return Mono.error(new IllegalArgumentException(
                    "protected side index token batch exceeds internal limit"));
        }
        if (parameterSets.isEmpty()) {
            return Mono.empty();
        }
        int parametersPerSet = parameterSets.getFirst().size();
        if (parametersPerSet == 0
                || parametersPerSet > ProtectedReplacementBatchPlan.MAX_PARAMETERS) {
            return Mono.error(new IllegalArgumentException(
                    "protected side index token batch exceeds internal limit"));
        }
        int batchSize = Math.min(
                MAX_TOKEN_BATCH_SIZE,
                ProtectedReplacementBatchPlan.MAX_PARAMETERS / parametersPerSet);
        int chunks = (parameterSets.size() + batchSize - 1) / batchSize;
        return Flux.range(0, chunks)
                   .concatMap(chunk -> executeParameterSets(
                           connection,
                           sql,
                           parameterSets.subList(
                                   chunk * batchSize,
                                   Math.min(parameterSets.size(), (chunk + 1) * batchSize))), 1)
                   .then();
    }

    static Mono<Void> deleteParameterSets(Connection connection,
                                          String sql,
                                          List<List<Object>> parameterSets) {
        if (parameterSets.size() > MAX_TOKEN_BATCH_SIZE
                || parameterSets.stream().mapToInt(List::size).sum()
                > ProtectedReplacementBatchPlan.MAX_PARAMETERS) {
            return Mono.error(new IllegalArgumentException(
                    "protected side index delete batch exceeds internal limit"));
        }
        if (parameterSets.isEmpty()) {
            return Mono.empty();
        }
        return Mono.defer(() -> {
            Statement statement = connection.createStatement(sql);
            for (int index = 0; index < parameterSets.size(); index++) {
                bind(statement, parameterSets.get(index));
                if (index + 1 < parameterSets.size()) {
                    statement.add();
                }
            }
            return Flux.from(statement.execute())
                       .concatMap(Result::getRowsUpdated, 1)
                       .then();
        });
    }

    private static Mono<Void> executeChunk(Connection connection,
                                           String sql,
                                           ProtectedWriteWork work,
                                           Map<String, Object> owner,
                                           ProtectedWriteWork.FieldTokens field,
                                           int offset,
                                           int limit) {
        return Mono.defer(() -> {
            Statement statement = connection.createStatement(sql);
            for (int index = offset; index < limit; index++) {
                bind(statement, work.sideIndexParameters(owner, field, index));
                if (index + 1 < limit) {
                    statement.add();
                }
            }
            return Flux.from(statement.execute())
                       .concatMap(Result::getRowsUpdated, 1)
                       .reduce(0L, R2dbcExecutionCounts::add)
                       .flatMap(total -> requireExactTotal(total, limit - offset));
        });
    }

    private static Mono<Void> executeParameterSets(Connection connection,
                                                   String sql,
                                                   List<List<Object>> parameterSets) {
        if (parameterSets.isEmpty()) {
            return Mono.empty();
        }
        return Mono.defer(() -> {
            Statement statement = connection.createStatement(sql);
            for (int index = 0; index < parameterSets.size(); index++) {
                bind(statement, parameterSets.get(index));
                if (index + 1 < parameterSets.size()) {
                    statement.add();
                }
            }
            return Flux.from(statement.execute())
                       .concatMap(Result::getRowsUpdated, 1)
                       .reduce(0L, R2dbcExecutionCounts::add)
                       .flatMap(total -> requireExactTotal(total, parameterSets.size()));
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

    private static Mono<Void> requireExactTotal(long affectedRows, int tokenCount) {
        if (affectedRows != tokenCount) {
            return Mono.error(new IllegalStateException(
                    "protected side index insert batch must affect one row per token"));
        }
        return Mono.empty();
    }
}
