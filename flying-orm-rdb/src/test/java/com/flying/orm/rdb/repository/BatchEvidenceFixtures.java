package com.flying.orm.rdb.repository;

import com.flying.orm.rdb.batch.BatchAffectedRows;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.result.DynamicRow;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.function.BiFunction;
import java.util.function.LongFunction;

/** Small execution-fact fixtures for Repository/Form contract tests. */
public final class BatchEvidenceFixtures {
    private BatchEvidenceFixtures() { }

    /** 只替代数据库 I/O，保留 Repository 到 Form 的真实规格、映射、绑定和回调链。 */
    static ReactiveFormClient batchFormClient(BiFunction<BatchWriteRequest,
            LongFunction<? extends Publisher<Void>>, Mono<BatchExecutionEvidence>> execute) {
        return batchFormClient(execute, com.flying.orm.core.codec.ValueCodecRegistry.standard());
    }

    static ReactiveFormClient batchFormClient(BiFunction<BatchWriteRequest,
            LongFunction<? extends Publisher<Void>>, Mono<BatchExecutionEvidence>> execute,
            com.flying.orm.core.codec.ValueCodecRegistry codecs) {
        return ReactiveFormClient.create(new ReactiveSqlExecutor() {
            public Flux<DynamicRow> query(SqlRequest request) { return Flux.error(new UnsupportedOperationException()); }
            public Mono<Long> rowsUpdated(SqlRequest request) { return Mono.error(new UnsupportedOperationException()); }
            public Mono<BatchExecutionEvidence> writeBatch(BatchWriteRequest request,
                    LongFunction<? extends Publisher<Void>> completed) {
                return execute.apply(request, completed);
            }
        }, com.flying.orm.rdb.form.FormDataSqlRenderer.create(
                com.flying.orm.core.sql.render.SqlRenderer.builder().addDefaultTerms().valueCodecs(codecs).build(),
                com.flying.orm.rdb.dialect.RdbDialect.postgresql()));
    }

    /** 行转换单测使用真正的表单客户端；未声明的 SQL 执行必须失败，不能被假成功隐藏。 */
    static com.flying.orm.rdb.form.ReactiveFormClient unusedFormClient() {
        return com.flying.orm.rdb.form.ReactiveFormClient.create(new com.flying.orm.rdb.reactive.ReactiveSqlExecutor() {
            public reactor.core.publisher.Flux<com.flying.orm.rdb.result.DynamicRow> query(com.flying.orm.core.sql.render.SqlRequest request) {
                return reactor.core.publisher.Flux.error(new UnsupportedOperationException());
            }
            public reactor.core.publisher.Mono<Long> rowsUpdated(com.flying.orm.core.sql.render.SqlRequest request) {
                return reactor.core.publisher.Mono.error(new UnsupportedOperationException());
            }
        }, com.flying.orm.rdb.form.FormDataSqlRenderer.create(
                com.flying.orm.core.sql.render.SqlRenderer.builder().addDefaultTerms().build(),
                com.flying.orm.rdb.dialect.RdbDialect.postgresql()));
    }

    public static BatchExecutionEvidence successful(long rows) {
        BatchExecutionEvidence.Accumulator facts = new BatchExecutionEvidence.Accumulator();
        facts.accept(rows);
        facts.succeeded(rows, BatchAffectedRows.known(rows));
        return facts.snapshot(BatchExecutionState.SUCCESS, null);
    }
}
