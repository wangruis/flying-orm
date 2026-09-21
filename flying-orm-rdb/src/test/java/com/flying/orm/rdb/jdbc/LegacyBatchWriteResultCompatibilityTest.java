package com.flying.orm.rdb.jdbc;
import com.flying.orm.rdb.batch.*;
import com.flying.orm.rdb.lifecycle.*;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
/** 旧事务完成契约已清退；保留逐行 POST 的完成时点、失败汇聚及普通/evidence 一致性。 */
class LegacyBatchWriteResultCompatibilityTest {
    @Test void postRunsOnlyAfterStatementCleanup() {
        var state = new JdbcBatchEvidenceTestSupport.State();
        var offsets = new ArrayList<Long>();
        var fact = state.writer().writeBatch(request(),offset -> {
            assertEquals(1,state.statementsClosed.get()); offsets.add(offset);
        });
        assertEquals(List.of(0L,1L),offsets);
        assertEquals(2,fact.successfulCount());
    }
    @Test void postFailureInvokesRemainingCompletedRowsAndStopsNewInput() {
        var state = new JdbcBatchEvidenceTestSupport.State();
        var offsets = new ArrayList<Long>();
        var primary = new EntityPostWriteException(EntityLifecyclePhase.POST_PERSIST,1L,new IllegalStateException("post"));
        var failure = assertThrows(EntityPostWriteException.class, () -> state.writer().writeBatch(
                JdbcBatchEvidenceTestSupport.request(Flux.range(0,5).map(i -> new Object[]{i}),2),offset -> {
                    offsets.add(offset); throw primary;
                }));
        assertSame(primary,failure);
        assertEquals(List.of(0L,1L),offsets);
        assertEquals(1,state.executions.get());
        assertEquals(1,state.released.get());
        var attachment = assertInstanceOf(BatchExecutionEvidenceException.class,failure.getSuppressed()[0]);
        assertNull(attachment.getCause());
        assertEquals(2,attachment.evidence().successfulCount());
        assertEquals(2,attachment.evidence().inputCount());
    }
    private static BatchWriteRequest request() {
        return JdbcBatchEvidenceTestSupport.request(Flux.range(0,2).map(i -> new Object[]{i}),2);
    }
}
