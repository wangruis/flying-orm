package com.flying.orm.core.condition;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import org.junit.jupiter.api.Test;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StructuredConditionDeterministicFailureTest {

    @Test
    void synchronousSnapshotFailureDoesNotUnwrapAnAsyncFatalCause() {
        AbstractCollection<Object> failingValue = new AbstractCollection<>() {
            @Override
            public Iterator<Object> iterator() {
                return new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        throw new CompletionException(new TestVirtualMachineError());
                    }

                    @Override
                    public Object next() {
                        throw new AssertionError("hasNext must fail first");
                    }
                };
            }

            @Override
            public int size() {
                return 1;
            }
        };

        StructuredConditionException error = assertThrows(
                StructuredConditionException.class,
                () -> StructuredConditionInput.term("payload", "eq", failingValue));

        assertEquals(StructuredConditionErrorCode.VALUE_SHAPE_NOT_ALLOWED, error.code());
    }

    @Test
    void synchronousConversionDoesNotUnwrapAnAsyncFatalCause() {
        TestVirtualMachineError fatal = new TestVirtualMachineError();
        CompletionException wrapped = new CompletionException(fatal);
        StructuredConditionValueNormalizer normalizer = new StructuredConditionValueNormalizer(
                new ValueCodecRegistry(List.of(failingCodec(wrapped))));

        StructuredConditionException error = assertThrows(
                StructuredConditionException.class,
                () -> normalizer.normalize("7",
                                           DynamicField.of("sequence", "INTEGER"),
                                           StructuredConditionPolicy.defaults(),
                                           "$",
                                           "="));

        assertEquals(StructuredConditionErrorCode.VALUE_CONVERSION_FAILED, error.code());
        assertSame(wrapped, error.getCause());
    }

    @Test
    void synchronousConversionLetsDirectFatalErrorsPropagateNaturally() {
        TestVirtualMachineError fatal = new TestVirtualMachineError();
        StructuredConditionValueNormalizer normalizer = new StructuredConditionValueNormalizer(
                new ValueCodecRegistry(List.of(failingCodec(fatal))));

        assertSame(fatal,
                   assertThrows(TestVirtualMachineError.class,
                                () -> normalizer.normalize("7",
                                                           DynamicField.of("sequence", "INTEGER"),
                                                           StructuredConditionPolicy.defaults(),
                                                           "$",
                                                           "=")));
    }

    private static ValueCodec failingCodec(Throwable failure) {
        return new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == Integer.class;
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                if (failure instanceof RuntimeException runtime) {
                    throw runtime;
                }
                throw (VirtualMachineError) failure;
            }
        };
    }

    private static final class TestVirtualMachineError extends VirtualMachineError {
        private TestVirtualMachineError() {
            super("wrapped outside an async boundary");
        }
    }
}
