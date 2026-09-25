package com.flying.orm.rdb.reactive;
import com.flying.orm.rdb.batch.*;
import com.flying.orm.rdb.observation.*;
import reactor.core.publisher.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

import com.flying.orm.rdb.execution.*;
import com.flying.orm.core.sql.render.SqlRequest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.DynamicTest;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;
import java.util.function.Supplier;

class R2dbcFatalRecoveryTest {
    @TestFactory Stream<DynamicTest> sequenceCleanupFatalRemainsPrimaryAfterWorkAndRelease() {
        return Stream.of(false, true).flatMap(workFails -> Stream.of(false, true).flatMap(releaseFails ->
                Stream.of(false, true).map(wrapped -> DynamicTest.dynamicTest(
                        "sequence workFails=" + workFails + " fatalDuringRelease=" + releaseFails
                                + " wrapped=" + wrapped, () -> {
                            VirtualMachineError fatal = new VirtualMachineError("cleanup fatal") { };
                            Throwable signal = wrapped ? new CompletionException(fatal) : fatal;
                            var h = new R2dbcOrdinaryBatchConnectionTest.Harness(index -> {
                                if (index == 0 && workFails) return Mono.error(new IllegalStateException("work"));
                                if (index == 1 && !releaseFails) return Mono.error(signal);
                                return Mono.just(1L);
                            });
                            if (releaseFails) h.releaseFailure = signal;
                            SqlRequest work = new SqlRequest("update sample set value = 1", List.of());
                            SqlRequest cleanup = new SqlRequest("update sample set value = 2", List.of());
                            Throwable actual = failure(() -> h.executor().executeInConnection(
                                    new SqlExecutionSequence(List.of(), List.of(work), List.of(cleanup)),
                                    SqlExecutionOptions.safeDefaults()));
                            assertEquals(2, h.executions);
                            assertEquals(List.of(workFails || !releaseFails
                                    ? SignalType.ON_ERROR : SignalType.ON_COMPLETE), h.releases);
                            assertSame(fatal, actual);
                        }))));
    }

    @TestFactory Stream<DynamicTest> fatalSqlErrorReleasesConnectionAndStopsBatch() {
        return Stream.of(false, true).map(wrapped -> DynamicTest.dynamicTest(
                "fatal business error wrapped=" + wrapped, () -> {
                    VirtualMachineError fatal = new VirtualMachineError("driver fatal") { };
                    Throwable signal = wrapped ? new CompletionException(fatal) : fatal;
                    var h = new R2dbcOrdinaryBatchConnectionTest.Harness(index -> Flux.create(sink -> sink.error(signal)));
                    Throwable actual = failure(() -> h.executor().writeBatch(
                            R2dbcOrdinaryBatchConnectionTest.request(2, 1, BatchRowCountPolicy.ANY)));
                    assertSame(fatal, actual);
                    assertEquals(1, h.executions);
                    assertEquals(List.of(SignalType.ON_ERROR), h.releases);
                }));
    }

    @TestFactory Stream<DynamicTest> fatalBeforeAcquisitionDoesNotRelease() {
        return Stream.of(false, true).flatMap(input -> Stream.of(false, true).map(wrapped ->
                DynamicTest.dynamicTest("fatal early input=" + input + " wrapped=" + wrapped, () -> {
                    VirtualMachineError fatal = new VirtualMachineError("early fatal") { };
                    Throwable signal = wrapped ? new CompletionException(fatal) : fatal;
                    var h = new R2dbcOrdinaryBatchConnectionTest.Harness(index -> Mono.just(1L));
                    if (!input) h.getFailure = signal;
                    var rows = input ? Flux.<Object[]>error(signal) : Flux.<Object[]>just(new Object[]{1});
                    assertSame(fatal, failure(() -> h.executor().writeBatch(
                            R2dbcOrdinaryBatchConnectionTest.request(rows, 1, BatchRowCountPolicy.ANY))));
                    assertEquals(0, h.executions);
                    assertTrue(h.releases.isEmpty());
                })));
    }

    @TestFactory Stream<DynamicTest> protectedWriteFatalReleasesConnection() {
        return Stream.of(false, true).map(wrapped -> DynamicTest.dynamicTest(
                "fatal protected write wrapped=" + wrapped, () -> {
                    VirtualMachineError fatal = new VirtualMachineError("driver fatal") { };
                    Throwable signal = wrapped ? new CompletionException(fatal) : fatal;
                    var h = new R2dbcOrdinaryBatchConnectionTest.Harness(index -> Flux.create(sink -> sink.error(signal)));
                    var work = new ProtectedWriteWork(ProtectedWriteWork.Kind.INSERT,
                            new SqlRequest("insert into business_row(id) values (?)", List.of(7L)),
                            null, List.of("id"), Map.of("id", 7L), "id = ?",
                            "delete from tokens where id = ? and tag = ?",
                            "insert into tokens(id, tag, token) values (?, ?, ?)",
                            List.of(new ProtectedWriteWork.FieldTokens("phone", List.of(new byte[]{1}))));
                    assertSame(fatal, failure(() -> h.executor().protectedWrite(work, SqlExecutionOptions.safeDefaults())));
                    assertEquals(1, h.executions);
                    assertEquals(List.of(SignalType.ON_ERROR), h.releases);
                }));
    }

    private static Throwable failure(Supplier<Mono<?>> operation) {
        try {
            var terminal = operation.get().materialize().block();
            return terminal.getThrowable();
        } catch (VirtualMachineError fatal) {
            return fatal;
        }
    }
}
