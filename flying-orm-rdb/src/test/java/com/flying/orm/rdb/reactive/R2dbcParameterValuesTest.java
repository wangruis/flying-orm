package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.internal.binding.SqlNullParameter;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Type;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 参数判型和绑定必须读取同一个驱动值快照。 */
class R2dbcParameterValuesTest {

    @Test
    void convertsDriverNeutralTypedNullForSecondaryBindingPaths() {
        Parameter parameter = assertInstanceOf(Parameter.class,
                R2dbcParameterValues.forBinding(new SqlNullParameter(Long.class)));

        assertEquals(Long.class, parameter.getType().getJavaType());
        assertNull(parameter.getValue());
    }

    @Test
    void readsCustomParameterValueOnlyOnceForBinding() {
        AtomicInteger reads = new AtomicInteger();
        Parameter changing = new Parameter() {
            @Override
            public Type getType() {
                return R2dbcType.BLOB;
            }

            @Override
            public Object getValue() {
                return reads.incrementAndGet() <= 2 ? new byte[]{1, 2, 3} : null;
            }
        };

        assertInstanceOf(Blob.class, R2dbcParameterValues.forBinding(changing));
        assertEquals(1, reads.get());
    }

    @Test
    void readsCustomParameterValueOnlyOnceWhenDetectingLargeObjects() {
        AtomicInteger reads = new AtomicInteger();
        Parameter changing = new Parameter() {
            @Override
            public Type getType() {
                return R2dbcType.BLOB;
            }

            @Override
            public Object getValue() {
                return reads.incrementAndGet() == 1 ? new byte[]{1, 2, 3} : null;
            }
        };

        assertTrue(R2dbcParameterValues.isLargeObject(changing));
        assertEquals(1, reads.get());
    }

    @Test
    void freezesStatefulNonLargeObjectParametersBeforeTheDriverReadsThem() {
        AtomicInteger reads = new AtomicInteger();
        Parameter changing = new Parameter() {
            @Override
            public Type getType() {
                return R2dbcType.VARCHAR;
            }

            @Override
            public Object getValue() {
                return reads.incrementAndGet() == 1 ? "first" : "second";
            }
        };

        Parameter binding = assertInstanceOf(Parameter.class, R2dbcParameterValues.forBinding(changing));

        assertEquals("first", binding.getValue());
        assertEquals(1, reads.get());
    }

    @Test
    void preservesInOutDirectionWhenAdaptingLargeObjectParameters() {
        class InOutBlobParameter implements Parameter, Parameter.In, Parameter.Out {
            @Override
            public Type getType() {
                return R2dbcType.BLOB;
            }

            @Override
            public Object getValue() {
                return new byte[]{1, 2, 3};
            }
        }

        Object binding = R2dbcParameterValues.forBinding(new InOutBlobParameter());

        assertTrue(binding instanceof Parameter.In);
        assertTrue(binding instanceof Parameter.Out);
        Parameter parameter = assertInstanceOf(Parameter.class, binding);
        assertInstanceOf(Blob.class, parameter.getValue());
    }

    @Test
    void freezesBinaryParameterBeforeTheDriverConsumesItsBlob() {
        byte[] bytes = {1, 2, 3};
        Parameter parameter = new Parameter() {
            @Override
            public Type getType() {
                return R2dbcType.BLOB;
            }

            @Override
            public Object getValue() {
                return bytes;
            }
        };

        Blob blob = assertInstanceOf(Blob.class, R2dbcParameterValues.forBinding(parameter));
        bytes[0] = 9;
        ByteBuffer content = Flux.from(blob.stream()).blockFirst();

        assertEquals(1, content.get());
    }

    @Test
    void freezesCharacterParameterBeforeTheDriverConsumesItsClob() {
        StringBuilder text = new StringBuilder("original");
        Parameter parameter = new Parameter() {
            @Override
            public Type getType() {
                return R2dbcType.CLOB;
            }

            @Override
            public Object getValue() {
                return text;
            }
        };

        Clob clob = assertInstanceOf(Clob.class, R2dbcParameterValues.forBinding(parameter));
        text.replace(0, text.length(), "changed");
        CharSequence content = Flux.from(clob.stream()).blockFirst();

        assertEquals("original", content.toString());
    }

    @Test
    void executionSessionOwnsMutableValuesBeforeColdSubscription() {
        byte[] source = {1, 2, 3};

        List<Object> owned = R2dbcExecutionSession.snapshotExecutionParameters(
                new SqlRequest("select ?", List.of(source)));
        source[0] = 9;

        assertEquals(1, assertInstanceOf(byte[].class, owned.getFirst())[0]);
    }

    @Test
    void executionSessionOwnsMutablePayloadInsideParameterWrappers() {
        byte[] source = {1, 2, 3};
        Parameter parameter = io.r2dbc.spi.Parameters.in(R2dbcType.VARBINARY, source);

        List<Object> owned = R2dbcExecutionSession.snapshotExecutionParameters(
                new SqlRequest("select ?", List.of(parameter)));
        source[0] = 9;

        Parameter snapshot = assertInstanceOf(Parameter.class, owned.getFirst());
        assertEquals(1, assertInstanceOf(byte[].class, snapshot.getValue())[0]);
    }

    @Test
    void adaptsAnOwnedBinaryLobThroughIndependentReadOnlyViews() {
        byte[] source = {1, 2, 3};
        Parameter parameter = io.r2dbc.spi.Parameters.in(R2dbcType.BLOB, source);

        Parameter owned = assertInstanceOf(
                Parameter.class, R2dbcParameterValues.snapshotForExecution(parameter));
        ByteBuffer ownedPayload = assertInstanceOf(ByteBuffer.class, owned.getValue());
        source[0] = 9;
        Blob blob = assertInstanceOf(Blob.class, R2dbcParameterValues.forBinding(owned));
        ByteBuffer boundView = Flux.from(blob.stream()).blockFirst();

        assertEquals(1, ownedPayload.get(0));
        assertTrue(ownedPayload.isReadOnly());
        assertTrue(boundView.isReadOnly());
        assertNotSame(ownedPayload, boundView);
        boundView.position(1);
        assertEquals(0, ownedPayload.position());
    }

    @Test
    void executionSessionReusesTheRequestBoundaryScalarSnapshot() {
        SqlRequest request = new SqlRequest("select ?, ?", List.of("stable", 7L));

        assertSame(request.parameters(), R2dbcExecutionSession.snapshotExecutionParameters(request));
    }
}
