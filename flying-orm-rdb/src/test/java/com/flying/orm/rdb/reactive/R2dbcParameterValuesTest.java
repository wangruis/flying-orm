package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.codec.SqlTypedValue;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

/** 验证驱动适配只转换字符 LOB，不碰普通参数。 */
class R2dbcParameterValuesTest {

    @Test
    void convertsTypedCharacterLobToNonBlockingClob() {
        Clob clob = assertInstanceOf(Clob.class,
                                     R2dbcParameterValues.forBinding(
                                             new SqlTypedValue(SqlTypedValue.Kind.CLOB, "large text")));

        StepVerifier.create(clob.stream())
                    .expectNext("large text")
                    .verifyComplete();
    }

    @Test
    void convertsTypedBinaryLobToNonBlockingBlob() {
        Blob blob = assertInstanceOf(Blob.class,
                                     R2dbcParameterValues.forBinding(
                                             new SqlTypedValue(SqlTypedValue.Kind.BLOB, new byte[]{1, 2, 3})));

        StepVerifier.create(blob.stream())
                    .assertNext(buffer -> {
                        ByteBuffer readable = buffer.duplicate();
                        byte[] actual = new byte[readable.remaining()];
                        readable.get(actual);
                        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[]{1, 2, 3}, actual);
                    })
                    .verifyComplete();
    }

    @Test
    void leavesOrdinaryValuesUntouched() {
        String value = "ordinary text";

        assertSame(value, R2dbcParameterValues.forBinding(value));
    }
}
