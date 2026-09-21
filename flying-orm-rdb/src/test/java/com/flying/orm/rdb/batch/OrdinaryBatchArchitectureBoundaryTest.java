package com.flying.orm.rdb.batch;

import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrdinaryBatchArchitectureBoundaryTest {

    @Test
    void optionsOnlyDescribeInputBufferLimits() {
        assertEquals(List.of("bufferSize", "maxRows", "maxBufferedBytes", "maxRowBytes"),
                     components(BatchWriteOptions.class));
        assertEquals(List.of("maxBufferSize", "maxBufferedBytes"), components(BatchMemoryLimits.class));
    }

    @Test
    void requestHasNoTransactionCompletionChannel() {
        assertEquals(List.of("statement", "parameterTypes", "rows", "options", "rowCountPolicy", "generatedKeys"),
                     components(BatchWriteRequest.class));
    }

    @Test
    void evidenceDoesNotExposeTransactionsOrBufferResults() {
        List<String> methods = Arrays.stream(BatchExecutionEvidence.class.getDeclaredMethods())
                                     .map(Method::getName).toList();
        assertFalse(methods.contains("mode"));
        assertFalse(methods.contains("commitFact"));
        assertFalse(methods.contains("chunks"));
        assertTrue(Arrays.stream(BatchExecutionEvidence.class.getDeclaredMethods())
                         .anyMatch(method -> method.getName().equals("successfulOffsets")
                                 && method.getReturnType() == LongStream.class));
        assertTrue(Arrays.stream(BatchExecutionEvidence.class.getDeclaredMethods())
                         .anyMatch(method -> method.getName().equals("failedOffsets")
                                 && method.getReturnType() == LongStream.class));
    }

    @Test
    void syncBatchHasOneExecutionFactAndOffsetCallbackContract() throws Exception {
        assertEquals(BatchExecutionEvidence.class,
                     SyncBatchExecutor.class.getMethod("writeBatch", BatchWriteRequest.class).getReturnType());
        assertTrue(Arrays.stream(SyncBatchExecutor.class.getMethods())
                         .anyMatch(method -> method.getName().equals("writeBatch")
                                 && Arrays.equals(method.getParameterTypes(),
                                                  new Class<?>[]{BatchWriteRequest.class, LongConsumer.class})));
        assertFalse(Arrays.stream(SyncBatchExecutor.class.getMethods())
                          .anyMatch(method -> method.getName().endsWith("Chunks")));
    }

    @Test
    void reactiveBatchHasOffsetCallbacksButNoPublicChunks() {
        assertTrue(Arrays.stream(ReactiveSqlExecutor.class.getMethods())
                         .anyMatch(method -> method.getName().equals("writeBatch")
                                 && Arrays.equals(method.getParameterTypes(),
                                                  new Class<?>[]{BatchWriteRequest.class, LongFunction.class})));
        assertFalse(Arrays.stream(ReactiveSqlExecutor.class.getMethods())
                          .anyMatch(method -> method.getName().endsWith("Chunks")));
    }

    private static List<String> components(Class<?> type) {
        return Arrays.stream(type.getRecordComponents()).map(component -> component.getName()).toList();
    }
}
