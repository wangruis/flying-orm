package com.flying.orm.rdb.execution;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class ProtectedExecutionValueSnapshotTest {

    @Test
    void protectedWriteFreezesMutableKnownOwner() {
        ByteBuffer owner = ByteBuffer.wrap(new byte[]{1});
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into users (id) values (?)", List.of(owner)),
                null,
                List.of("id"),
                Map.of("id", owner),
                "id = ?",
                "delete from user_tokens where id = ?",
                "insert into user_tokens (id, token) values (?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("phone", List.of(new byte[]{2}))));

        owner.put(0, (byte) 9);

        ByteBuffer published = (ByteBuffer) work.knownOwner().get("id");
        assertEquals(1, published.get(0));
        assertTrue(published.isReadOnly());
        ByteBuffer requestValue = (ByteBuffer) work.writeRequest().parameters().getFirst();
        assertEquals(1, requestValue.get(0));
        assertTrue(requestValue.isReadOnly());
    }

    @Test
    void protectedBatchFreezesMutableReceiptIdentity() {
        ByteBuffer receipt = ByteBuffer.wrap(new byte[]{1});
        Object[] row = ProtectedBatchRows.extend(new Object[]{"value"}, null, new Object[]{receipt});

        receipt.put(0, (byte) 9);

        ByteBuffer published = (ByteBuffer) ProtectedBatchRows.receiptParameters(row, 1)[0];
        assertEquals(1, published.get(0));
        assertTrue(published.isReadOnly());
    }

    @Test
    void shallowScalarSnapshotHonorsTheByteBudgetBeforePublishingTheRow() {
        Object[] row = {"0123456789"};

        BatchMemoryLimitExceededException failure = assertThrows(
                BatchMemoryLimitExceededException.class,
                () -> BatchRowSnapshotter.snapshotAndEstimate(row, 1, 32L, "test bytes"));

        assertEquals("test bytes", failure.limitName());
        assertEquals(32L, failure.limit());
        assertTrue(failure.actual() > failure.limit());
    }

    @Test
    void batchOwnershipPassesThroughTheRowAndItsValues() {
        ByteBuffer shared = ByteBuffer.wrap(new byte[]{1, 2});
        Object[] row = {shared, shared};

        Object[] owned = BatchRowSnapshotter.snapshot(row);

        assertSame(row, owned);
        assertSame(shared, owned[0]);
        assertSame(owned[0], owned[1]);
    }

    @Test
    void batchSnapshotReusesAlreadyOwnedProtectedWork() {
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into users (id) values (?)", List.of(1L)),
                null,
                List.of("id"),
                Map.of("id", 1L),
                "id = ?",
                "delete from user_tokens where id = ?",
                "insert into user_tokens (id, token) values (?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("phone", List.of(new byte[]{2}))));
        Object[] row = ProtectedBatchRows.extend(new Object[]{1L}, work);

        Object[] snapshot = BatchRowSnapshotter.snapshot(row, 1, 4096L, "test bytes");

        assertSame(work, ProtectedBatchRows.work(snapshot, 1));
    }

    @Test
    void protectedTokenPublicationRemainsDefensive() {
        byte[] source = {1, 2, 3};
        ProtectedWriteWork.FieldTokens field =
                new ProtectedWriteWork.FieldTokens("phone", List.of(source));
        source[0] = 8;
        List<byte[]> publicTokens = field.tokens();

        assertNotSame(source, publicTokens.getFirst());
        assertEquals(1, publicTokens.getFirst()[0]);
        publicTokens.getFirst()[0] = 9;
        assertEquals(1, field.tokens().getFirst()[0]);
    }

    @Test
    void serializedTimeoutExceptionRetainsItsPublicTimeoutContract() throws Exception {
        Duration timeout = Duration.ofMillis(1250);
        SqlExecutionTimeoutException original = new SqlExecutionTimeoutException(
                timeout, new TimeoutException("deadline elapsed"));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(original);
        }

        SqlExecutionTimeoutException restored;
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (SqlExecutionTimeoutException) input.readObject();
        }

        assertEquals(timeout, restored.timeout());
        assertEquals(timeout.toString(), restored.toErrorReport().resource());
    }
}
