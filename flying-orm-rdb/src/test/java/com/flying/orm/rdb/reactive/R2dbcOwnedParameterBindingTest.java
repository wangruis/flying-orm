package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.codec.SqlTypedValue;
import com.flying.orm.rdb.internal.binding.SqlNullParameter;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Type;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class R2dbcOwnedParameterBindingTest {

    @Test
    void ownedBlobBindingDoesNotAllocateAnotherPayloadArray() {
        com.sun.management.ThreadMXBean allocationBean = assertInstanceOf(
                com.sun.management.ThreadMXBean.class, ManagementFactory.getThreadMXBean());
        if (!allocationBean.isThreadAllocatedMemoryEnabled()) {
            allocationBean.setThreadAllocatedMemoryEnabled(true);
        }
        List<Object> parameters = List.of(
                Parameters.in(R2dbcType.BLOB, new byte[1_048_576]),
                Parameters.in(R2dbcType.BLOB, ByteBuffer.allocate(1_048_576)),
                Parameters.inOut(R2dbcType.BLOB, new byte[1_048_576]),
                new SqlTypedValue(SqlTypedValue.Kind.BLOB, new byte[1_048_576]));
        long threadId = Thread.currentThread().threadId();
        for (Object parameter : parameters) {
            for (int iteration = 0; iteration < 16; iteration++) {
                Blob blob = blobValue(R2dbcParameterValues.forOwnedBinding(parameter));
                Mono.from(blob.discard()).block();
            }

            long before = allocationBean.getThreadAllocatedBytes(threadId);
            Object binding = R2dbcParameterValues.forOwnedBinding(parameter);
            long allocated = allocationBean.getThreadAllocatedBytes(threadId) - before;

            Blob blob = blobValue(binding);
            ByteBuffer content = Flux.from(blob.stream()).blockFirst();
            assertEquals(1_048_576, content.remaining());
            assertTrue(content.isReadOnly());
            assertTrue(allocated < 65_536L,
                    () -> "owned BLOB binding allocated another payload: " + allocated + " bytes");
        }
    }

    @Test
    void ownedBlobBindingsPreserveTheBufferWindowAndIndependentDriverPositions() {
        ByteBuffer source = ByteBuffer.wrap(new byte[]{9, 1, 2, 3, 8});
        source.position(1);
        source.limit(4);
        ByteBuffer slice = source.slice();
        slice.position(1);
        List<Object> parameters = List.of(
                Parameters.in(R2dbcType.BLOB, slice),
                Parameters.inOut(R2dbcType.BLOB, slice),
                new SqlTypedValue(SqlTypedValue.Kind.BLOB, slice));

        for (Object parameter : parameters) {
            Object binding = R2dbcParameterValues.forOwnedBinding(parameter);
            if (parameter instanceof Parameter.Out) {
                assertTrue(binding instanceof Parameter.In);
                assertTrue(binding instanceof Parameter.Out);
                assertEquals(R2dbcType.BLOB, ((Parameter) binding).getType());
            }
            ByteBuffer content = Flux.from(blobValue(binding).stream()).blockFirst();
            assertTrue(content.isReadOnly());
            assertEquals(2, content.remaining());
            byte[] actual = new byte[content.remaining()];
            content.get(actual);
            assertArrayEquals(new byte[]{2, 3}, actual);
            assertEquals(1, slice.position());
            assertEquals(3, slice.limit());
            assertEquals(1, source.position());
            assertEquals(4, source.limit());
        }
    }

    @Test
    void ownedOutputBlobReadsEachAccessorOnceAndRetainsOutputDirection() {
        AtomicInteger valueReads = new AtomicInteger();
        AtomicInteger typeReads = new AtomicInteger();
        byte[] payload = {1, 2, 3};
        class OutputBlob implements Parameter, Parameter.Out {
            @Override
            public Type getType() {
                assertEquals(1, typeReads.incrementAndGet());
                return R2dbcType.BLOB;
            }

            @Override
            public Object getValue() {
                assertEquals(1, valueReads.incrementAndGet());
                return payload;
            }
        }

        Parameter binding = assertInstanceOf(Parameter.class,
                R2dbcParameterValues.forOwnedBinding(new OutputBlob()));

        assertTrue(binding instanceof Parameter.Out);
        assertFalse(binding instanceof Parameter.In);
        assertEquals(R2dbcType.BLOB, binding.getType());
        ByteBuffer content = Flux.from(blobValue(binding).stream()).blockFirst();
        byte[] actual = new byte[content.remaining()];
        content.get(actual);
        assertArrayEquals(new byte[]{1, 2, 3}, actual);
        assertEquals(1, valueReads.get());
        assertEquals(1, typeReads.get());
    }

    @Test
    void ordinaryColdBlobExecutionsStillOwnAnIndependentPayloadSnapshot() {
        byte[] source = {1, 2, 3};
        List<Object> parameters = List.of(
                Parameters.in(R2dbcType.BLOB, source),
                Parameters.inOut(R2dbcType.BLOB, source),
                new SqlTypedValue(SqlTypedValue.Kind.BLOB, source));
        List<Object> snapshots = parameters.stream()
                .map(R2dbcParameterValues::snapshotForExecution).toList();
        source[0] = 9;

        for (Object snapshot : snapshots) {
            Object binding = R2dbcParameterValues.forBinding(snapshot);
            ByteBuffer content = Flux.from(blobValue(binding).stream()).blockFirst();
            byte[] actual = new byte[content.remaining()];
            content.get(actual);
            assertArrayEquals(new byte[]{1, 2, 3}, actual);
            assertTrue(content.isReadOnly());
        }
    }

    @Test
    void ownedNonBinaryBindingsRetainNullClobAndExistingLobValues() {
        Parameter typedNull = assertInstanceOf(Parameter.class,
                R2dbcParameterValues.forOwnedBinding(new SqlNullParameter(Long.class)));
        assertEquals(Long.class, typedNull.getType().getJavaType());
        assertNull(typedNull.getValue());
        Parameter outNull = assertInstanceOf(Parameter.class,
                R2dbcParameterValues.forOwnedBinding(Parameters.out(R2dbcType.BLOB)));
        assertTrue(outNull instanceof Parameter.Out);
        assertFalse(outNull instanceof Parameter.In);
        assertEquals(R2dbcType.BLOB, outNull.getType());
        assertNull(outNull.getValue());

        Parameter textBinding = assertInstanceOf(Parameter.class,
                R2dbcParameterValues.forOwnedBinding(Parameters.inOut(R2dbcType.NCLOB, "text")));
        assertTrue(textBinding instanceof Parameter.In);
        assertTrue(textBinding instanceof Parameter.Out);
        assertEquals(R2dbcType.NCLOB, textBinding.getType());
        Clob clob = assertInstanceOf(Clob.class, textBinding.getValue());
        assertEquals("text", Flux.from(clob.stream()).blockFirst());

        Blob suppliedBlob = Blob.from(Mono.just(ByteBuffer.wrap(new byte[]{7})));
        Clob suppliedClob = Clob.from(Mono.just("supplied"));
        Parameter blobBinding = assertInstanceOf(Parameter.class,
                R2dbcParameterValues.forOwnedBinding(Parameters.in(R2dbcType.BLOB, suppliedBlob)));
        Parameter clobBinding = assertInstanceOf(Parameter.class,
                R2dbcParameterValues.forOwnedBinding(Parameters.in(R2dbcType.CLOB, suppliedClob)));
        assertSame(suppliedBlob, blobBinding.getValue());
        assertSame(suppliedClob, clobBinding.getValue());
        Mono.from(suppliedBlob.discard()).block();
        Mono.from(suppliedClob.discard()).block();
    }

    private static Blob blobValue(Object binding) {
        return assertInstanceOf(Blob.class, binding instanceof Parameter parameter ? parameter.getValue() : binding);
    }
}
