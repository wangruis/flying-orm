package com.flying.orm.rdb.jdbc;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;

import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcBatchEvidenceErrorTest {

    @TestFactory
    Stream<DynamicTest> preservesUpstreamErrorWithoutCompletingExternalTransaction() {
        return Stream.of(new AssertionError("upstream failed"), new SyntheticVirtualMachineError())
                .map(error -> DynamicTest.dynamicTest(error.getClass().getSimpleName(), () -> {
                    JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State();

                    Error thrown = assertThrows(Error.class, () -> state.writer()

                            .writeBatchEvidence(JdbcBatchEvidenceTestSupport.request(afterOneRow(error), 1)));

                    assertAll(
                            () -> assertSame(error, thrown),
                            () -> assertEquals(1, state.executions.get()),
                            () -> assertEquals(0, state.commits.get()),
                            () -> assertEquals(0, state.rollbacks.get()),
                            () -> assertEquals(0, state.closes.get()));
                }));
    }

    @Test
    void preservesChunkExecutionErrorWithoutCompletingExternalTransaction() {
        AssertionError error = new AssertionError("driver execution failed");
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State().failure(error);

        assertSame(error, assertThrows(AssertionError.class, () -> state.writer()

                .writeBatchEvidence(JdbcBatchEvidenceTestSupport.request(Flux.<Object[]>just(new Object[]{1}), 1))));

        assertEquals(1, state.executions.get());
        assertEquals(0, state.commits.get());
        assertEquals(0, state.rollbacks.get());
        assertEquals(0, state.closes.get());
    }

    @Test
    void externalErrorNeverTakesOwnershipOfTheUpperLayerTransaction() {
        AssertionError error = new AssertionError("upstream failed");
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State();


        JdbcBatchWriter writer = state.writer()
                ;

        assertSame(error, assertThrows(AssertionError.class,
                () -> writer.writeBatchEvidence(JdbcBatchEvidenceTestSupport.request(afterOneRow(error), 1))));

        assertAll(
                () -> assertEquals(1, state.executions.get()),
                () -> assertEquals(0, state.autoCommitReads.get()),
                () -> assertEquals(0, state.autoCommitWrites.get()),
                () -> assertEquals(0, state.commits.get()),
                () -> assertEquals(0, state.rollbacks.get()),
                () -> assertEquals(0, state.closes.get()));
    }

    private static Flux<Object[]> afterOneRow(Error error) {
        return Flux.concat(Flux.<Object[]>just(new Object[]{1}), Flux.error(error));
    }

    private static final class SyntheticVirtualMachineError extends VirtualMachineError {
    }
}
