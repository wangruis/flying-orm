package com.flying.orm.rdb.jdbc;
import com.flying.orm.rdb.batch.*;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
class JdbcBatchIncompleteChunkInputCountTest {
    @Test void receivedTailDoesNotEraseEarlierExecution() {
        var state = new JdbcBatchEvidenceTestSupport.State();
        var input = Flux.concat(Flux.range(0, 3).map(i -> new Object[]{i}),
                Flux.error(new IllegalArgumentException("input")));
        var failure = assertThrows(BatchExecutionEvidenceException.class, () -> state.writer().writeBatch(
                JdbcBatchEvidenceTestSupport.request(input, 2)));
        assertEquals(3, failure.evidence().inputCount());
        assertEquals(List.of(0L,1L), failure.evidence().successfulOffsets().boxed().toList());
        assertEquals(0, failure.evidence().failedCount());
        assertEquals(2, failure.evidence().affectedRows().value());
        assertEquals(1, state.executions.get());
        assertEquals(1, state.released.get());
    }
}
