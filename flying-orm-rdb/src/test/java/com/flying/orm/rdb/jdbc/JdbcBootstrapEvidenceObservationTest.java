package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.batch.BatchCommitFact;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.sql.BatchUpdateException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcBootstrapEvidenceObservationTest {

    @Test
    void forwardsSuccessfulEvidenceThroughBootstrapWithEnabledOrDisabledSqlObserver() {
        for (SqlExecutionObserver sqlObserver : List.<SqlExecutionObserver>of(
                SqlExecutionObserver.noop(), observation -> { })) {
            JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State().outcome(1);
            EvidenceObserver observer = new EvidenceObserver();
            try (FlyingOrmClients clients = FlyingOrmClients.builder(state.dataSource())
                    .configuredDialect("h2")
                    .observers(sqlObserver, observer)
                    .build()) {
                BatchExecutionEvidence evidence = clients.syncForms().writeBatchEvidence(insertSpec(1));

                assertEquals(BatchCommitFact.COMMITTED, evidence.commitFact());
                assertEquals(1, state.executions.get());
                assertEquals(1, observer.evidence.size());
                assertSame(evidence, observer.evidence.getFirst());
            }
        }
    }

    @Test
    void forwardsPartialEvidenceThroughBootstrapAfterRollback() {
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State()
                .failure(new BatchUpdateException("second row failed", "23505", 0, new int[]{1}));
        EvidenceObserver observer = new EvidenceObserver();
        try (FlyingOrmClients clients = FlyingOrmClients.builder(state.dataSource())
                .configuredDialect("h2")
                .observers(SqlExecutionObserver.noop(), observer)
                .build()) {
            BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                    () -> clients.syncForms().writeBatchEvidence(insertSpec(2)));

            assertEquals(BatchCommitFact.ROLLED_BACK, failure.evidence().commitFact());
            assertEquals(1, failure.evidence().chunks().getFirst().successfulCount());
            assertEquals(1, state.rollbacks.get());
            assertEquals(1, observer.evidence.size());
            assertSame(failure.evidence(), observer.evidence.getFirst());
        }
    }

    @Test
    void directJdbcWriterAlreadyPublishesTheSameEvidence() {
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State().outcome(1);
        EvidenceObserver observer = new EvidenceObserver();
        BatchExecutionEvidence evidence = JdbcBatchWriter.create(state.dataSource())
                .withBatchObserver(observer)
                .writeBatchEvidence(JdbcBatchEvidenceTestSupport.request(
                        Flux.<Object[]>just(new Object[]{1}), 1));

        assertEquals(1, observer.evidence.size());
        assertSame(evidence, observer.evidence.getFirst());
    }

    private static BatchSpec insertSpec(int rows) {
        DynamicForm form = DynamicForm.builder("samples", "samples")
                .addField(DynamicField.of("value", "INTEGER"))
                .build();
        return BatchSpec.insert(form, Flux.range(1, rows).map(value -> Map.<String, Object>of("value", value)))
                .withOptions(BatchWriteOptions.atomic(rows));
    }

    private static final class EvidenceObserver implements BatchExecutionObserver {
        private final List<BatchExecutionEvidence> evidence = new ArrayList<>();

        @Override
        public void onExecution(BatchExecutionObservation observation) {
        }

        @Override
        public void onExecutionEvidence(BatchExecutionEvidence observed) {
            evidence.add(observed);
        }
    }
}
