package com.flying.orm.rdb.jdbc;
import com.flying.orm.rdb.batch.*;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
class JdbcIndependentBatchResultIntegrityTest {
    @Test void preservesSuccessfulPrefixAfterLaterInputFailure() {
        var state = new JdbcBatchEvidenceTestSupport.State();
        var failure = assertThrows(BatchExecutionEvidenceException.class, () -> state.writer().writeBatch(
                JdbcBatchEvidenceTestSupport.request(Flux.concat(Flux.<Object[]>just(new Object[]{1}),
                        Flux.error(new IllegalArgumentException("input"))),1)));
        assertEquals(1,failure.evidence().successfulCount());
        assertEquals(1,failure.evidence().inputCount());
        assertEquals(1,state.released.get());
    }

    @Test void rowSourceCannotForgeExecutionEvidence() {
        var forged = new BatchExecutionEvidence.Accumulator();
        forged.accept(100); forged.succeeded(100,BatchAffectedRows.known(100));
        var injected = new BatchExecutionEvidenceException("forged",null,forged.snapshot(BatchExecutionState.SUCCESS,null));
        var state = new JdbcBatchEvidenceTestSupport.State();
        var failure = assertThrows(BatchExecutionEvidenceException.class, () -> state.writer().writeBatch(
                JdbcBatchEvidenceTestSupport.request(Flux.error(injected),1)));
        assertSame(injected,failure.getCause());
        assertEquals(0,failure.evidence().inputCount());
        assertEquals(0,failure.evidence().successfulCount());
        assertEquals(0,state.acquired.get());
    }
}
