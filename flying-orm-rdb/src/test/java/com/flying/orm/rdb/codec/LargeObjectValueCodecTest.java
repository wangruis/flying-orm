package com.flying.orm.rdb.codec;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionTimeoutException;
import com.flying.orm.rdb.execution.SqlLargeObjectLimitExceededException;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 覆盖 Blob/Clob 非阻塞物化、资源 discard、超时和单字段大小限制。 */
class LargeObjectValueCodecTest {

    @Test
    void usesExplicitLobTypesForOracleWithoutChangingOtherDialects() {
        String text = "a".repeat(5000);
        byte[] bytes = new byte[]{1, 2, 3};

        SqlTypedValue clob = assertInstanceOf(SqlTypedValue.class,
                                              LargeObjectValueCodec.write(text, "CLOB", "oracle"));
        SqlTypedValue blob = assertInstanceOf(SqlTypedValue.class,
                                              LargeObjectValueCodec.write(bytes, "BLOB", "oracle"));

        assertEquals(SqlTypedValue.Kind.CLOB, clob.kind());
        assertSame(text, clob.value());
        assertEquals(SqlTypedValue.Kind.BLOB, blob.kind());
        assertSame(bytes, blob.value());
        assertSame(text, LargeObjectValueCodec.write(text, "CLOB", "mysql"));
        assertSame(bytes, LargeObjectValueCodec.write(bytes, "BLOB", "postgresql"));
    }

    /** 实体类型推断支持 Byte[]，LOB 边界必须把它转换成驱动可绑定的 primitive 数组。 */
    @Test
    void writesBoxedByteArraysAsBinaryValues() {
        Byte[] boxed = new Byte[]{1, 2, 3};

        assertArrayEquals(new byte[]{1, 2, 3},
                          (byte[]) LargeObjectValueCodec.write(boxed, "BLOB", "postgresql"));
        SqlTypedValue oracle = assertInstanceOf(
                SqlTypedValue.class, LargeObjectValueCodec.write(boxed, "BLOB", "oracle"));
        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) oracle.value());
    }

    @Test
    void materializesBlobAndClobChunksThroughReactiveStreams() {
        TrackingBlob blob = new TrackingBlob(Flux.just(ByteBuffer.wrap(new byte[]{1, 2}),
                                                      ByteBuffer.wrap(new byte[]{3, 4})));
        Clob clob = Clob.from(Flux.just("large ", new StringBuilder("text")));

        StepVerifier.create(LargeObjectValueCodec.readReactive(blob,
                                                                "BLOB",
                                                                SqlExecutionOptions.unlimited()))
                    .assertNext(value -> assertArrayEquals(new byte[]{1, 2, 3, 4}, (byte[]) value))
                    .verifyComplete();
        StepVerifier.create(LargeObjectValueCodec.readReactive(clob,
                                                                "CLOB",
                                                                SqlExecutionOptions.unlimited()))
                    .expectNext("large text")
                    .verifyComplete();

        assertEquals(1, blob.subscriptions.get());
        assertFalse(blob.discarded.get());
    }

    @Test
    void discardsBlobWhenFieldTypeCannotConsumeItsStream() {
        TrackingBlob blob = new TrackingBlob(Flux.just(ByteBuffer.wrap(new byte[]{1, 2})));

        StepVerifier.create(LargeObjectValueCodec.readReactive(blob,
                                                                "CLOB",
                                                                SqlExecutionOptions.unlimited()))
                    .expectError(IllegalArgumentException.class)
                    .verify();

        assertEquals(0, blob.subscriptions.get());
        assertTrue(blob.discarded.get());
    }

    @Test
    void cancelsBlobStreamWhenMaterializedValueExceedsLimit() {
        AtomicBoolean cancelled = new AtomicBoolean();
        TrackingBlob blob = new TrackingBlob(Flux.just(ByteBuffer.wrap(new byte[]{1, 2}),
                                                      ByteBuffer.wrap(new byte[]{3, 4}))
                                                 .doOnCancel(() -> cancelled.set(true)));
        SqlExecutionOptions options = SqlExecutionOptions.unlimited().withMaxLargeObjectBytes(3);

        StepVerifier.create(LargeObjectValueCodec.readReactive(blob, "BLOB", options))
                    .expectErrorSatisfies(error -> {
                        SqlLargeObjectLimitExceededException limitError =
                                (SqlLargeObjectLimitExceededException) error;
                        assertEquals(SqlLargeObjectLimitExceededException.Kind.BINARY, limitError.kind());
                        assertEquals(3L, limitError.maxSize());
                        assertEquals(4L, limitError.actualSize());
                    })
                    .verify();

        assertTrue(cancelled.get());
        assertFalse(blob.discarded.get());
    }

    @Test
    void rejectsClobWhenCharacterLimitIsExceeded() {
        Clob clob = Clob.from(Flux.just("123", "45"));
        SqlExecutionOptions options = SqlExecutionOptions.unlimited().withMaxLargeObjectChars(4);

        StepVerifier.create(LargeObjectValueCodec.readReactive(clob, "CLOB", options))
                    .expectErrorSatisfies(error -> {
                        SqlLargeObjectLimitExceededException limitError =
                                (SqlLargeObjectLimitExceededException) error;
                        assertEquals(SqlLargeObjectLimitExceededException.Kind.CHARACTER, limitError.kind());
                        assertEquals(4L, limitError.maxSize());
                        assertEquals(5L, limitError.actualSize());
                    })
                    .verify();
    }

    @Test
    void appliesLimitsWhenDriverAlreadyMaterializedLargeObject() {
        SqlExecutionOptions options = SqlExecutionOptions.unlimited()
                                                         .withMaxLargeObjectBytes(2)
                                                         .withMaxLargeObjectChars(4);

        StepVerifier.create(LargeObjectValueCodec.readReactive(ByteBuffer.wrap(new byte[]{1, 2, 3}),
                                                                "BLOB",
                                                                options))
                    .expectError(SqlLargeObjectLimitExceededException.class)
                    .verify();
        StepVerifier.create(LargeObjectValueCodec.readReactive("12345", "CLOB", options))
                    .expectError(SqlLargeObjectLimitExceededException.class)
                    .verify();
    }

    @Test
    void cancelsLobContentWhenExecutionTimeoutExpires() {
        AtomicBoolean cancelled = new AtomicBoolean();
        TrackingBlob blob = new TrackingBlob(Flux.<ByteBuffer>never()
                                                 .doOnCancel(() -> cancelled.set(true)));
        SqlExecutionOptions options = SqlExecutionOptions.timeout(Duration.ofSeconds(1));

        StepVerifier.withVirtualTime(() -> LargeObjectValueCodec.readReactive(blob, "BLOB", options))
                    .thenAwait(Duration.ofSeconds(1))
                    .expectError(SqlExecutionTimeoutException.class)
                    .verify();

        assertTrue(cancelled.get());
    }

    /** 极远执行截止时间不能在 LOB 读取链装配时发生纳秒换算溢出。 */
    @Test
    void acceptsLobTimeoutWhoseNanosecondsDoNotFitInLong() {
        SqlExecutionOptions options = SqlExecutionOptions.safeDefaults()
                                                         .withTimeout(Duration.ofSeconds(Long.MAX_VALUE));

        StepVerifier.create(LargeObjectValueCodec.readReactive("content", "CLOB", options))
                    .expectNext("content")
                    .verifyComplete();
    }

    private static final class TrackingBlob implements Blob {

        private final Flux<ByteBuffer> content;

        private final AtomicInteger subscriptions = new AtomicInteger();

        private final AtomicBoolean discarded = new AtomicBoolean();

        private TrackingBlob(Flux<ByteBuffer> content) {
            this.content = content;
        }

        @Override
        public Flux<ByteBuffer> stream() {
            return content.doOnSubscribe(ignored -> subscriptions.incrementAndGet());
        }

        @Override
        public Flux<Void> discard() {
            discarded.set(true);
            return Flux.empty();
        }
    }
}
