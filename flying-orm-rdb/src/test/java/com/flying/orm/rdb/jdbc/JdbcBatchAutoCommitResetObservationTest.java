package com.flying.orm.rdb.jdbc;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import static org.junit.jupiter.api.Assertions.*;
class JdbcBatchAutoCommitResetObservationTest {
    @Test void ordinaryBatchNeverInspectsOrChangesExternalConnectionState() {
        var state = new JdbcBatchEvidenceTestSupport.State();
        state.writer().writeBatch(JdbcBatchEvidenceTestSupport.request(Flux.range(0,5).map(i -> new Object[]{i}),2));
        assertEquals(0,state.autoCommitReads.get());
        assertEquals(0,state.autoCommitWrites.get());
        assertEquals(0,state.commits.get());
        assertEquals(0,state.rollbacks.get());
        assertEquals(0,state.closes.get());
        assertEquals(1,state.acquired.get());
        assertEquals(1,state.released.get());
    }
}
