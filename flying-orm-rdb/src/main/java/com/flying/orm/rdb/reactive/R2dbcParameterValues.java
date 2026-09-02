package com.flying.orm.rdb.reactive;

import com.flying.orm.core.internal.value.BindableValueSnapshots;
import com.flying.orm.rdb.codec.SqlTypedValue;
import com.flying.orm.rdb.internal.binding.SqlNullParameter;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Type;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;

/**
 * 在参数真正交给驱动前处理少数 R2DBC 驱动差异。
 *
 * <p>渲染阶段保留带类型的 BLOB/CLOB Parameter，批量摘要因此仍能读取稳定的 byte[] 或字符串内容。
 * Oracle 的 MERGE 源子查询无法从目标列推断大字段类型，但驱动能正确消费 R2DBC {@link Blob}/{@link Clob}。
 * 所以只在 bind 的最后一刻换成单次订阅的 LOB 句柄，整个过程仍是非阻塞的。</p>
 */
final class R2dbcParameterValues {

    private R2dbcParameterValues() {
    }

    static Object forBinding(Object value) {
        return forBinding(value, false);
    }

    /**
     * Binds a value whose mutable payload ownership has already moved into the current batch row.
     * Ordinary cold executions keep using {@link #forBinding(Object)} and its independent snapshot.
     */
    static Object forOwnedBinding(Object value) {
        return forBinding(value, true);
    }

    private static Object forBinding(Object value, boolean owned) {
        if (value instanceof SqlNullParameter typedNull) {
            return Parameters.in(typedNull.javaType());
        }
        if (value instanceof SqlTypedValue typedValue) {
            return typedValue.kind() == SqlTypedValue.Kind.BLOB
                    ? Blob.from(Mono.just(binaryContent(typedValue.value(), owned)))
                    : Clob.from(Mono.just(((CharSequence) typedValue.value()).toString()));
        }
        if (value instanceof Parameter parameter) {
            // 自定义 Parameter 可能用延迟或状态型访问器；同一次绑定只能读取一次值和类型。
            Object parameterValue = parameter.getValue();
            Type parameterType = parameter.getType();
            if (isCharacterLargeObject(parameterValue, parameterType)) {
                Object clob = Clob.from(Mono.just(parameterValue.toString()));
                return parameter instanceof Parameter.Out
                        ? stableParameter(parameter, parameterType, clob)
                        : clob;
            }
            if (isBinaryLargeObject(parameterValue, parameterType)) {
                ByteBuffer content = binaryContent(parameterValue,
                        owned || parameter instanceof StableParameter && parameterValue instanceof ByteBuffer);
                Object blob = Blob.from(Mono.just(content));
                return parameter instanceof Parameter.Out
                        ? stableParameter(parameter, parameterType, blob)
                        : blob;
            }
            return stableParameter(parameter, parameterType, parameterValue);
        }
        return value;
    }

    /** 在返回 cold Publisher 前冻结包装器和值；普通不可变标量保持零复制。 */
    static Object snapshotForExecution(Object value) {
        if (value instanceof SqlTypedValue typedValue) {
            Object payload = typedValue.value();
            if (typedValue.kind() == SqlTypedValue.Kind.BLOB) {
                return new StableInParameter(R2dbcType.BLOB, snapshotBinary(payload));
            }
            Object owned = BindableValueSnapshots.immutableValue(payload);
            return owned == payload ? typedValue : new SqlTypedValue(typedValue.kind(), owned);
        }
        if (value instanceof Parameter parameter) {
            Object payload = parameter.getValue();
            Type type = parameter.getType();
            Object owned = isBinaryLargeObject(payload, type)
                    ? snapshotBinary(payload) : BindableValueSnapshots.immutableValue(payload);
            return stableParameter(parameter, type, owned);
        }
        // SqlRequest has already taken ownership of ordinary bindable values. Only wrappers whose
        // payload is opaque to core need an execution-boundary snapshot here.
        return value;
    }

    static boolean isCharacterLargeObject(Object value) {
        if (!(value instanceof Parameter parameter)) {
            return false;
        }
        Object parameterValue = parameter.getValue();
        Type parameterType = parameter.getType();
        return isCharacterLargeObject(parameterValue, parameterType);
    }

    static boolean isBinaryLargeObject(Object value) {
        if (!(value instanceof Parameter parameter)) {
            return false;
        }
        Object parameterValue = parameter.getValue();
        Type parameterType = parameter.getType();
        return isBinaryLargeObject(parameterValue, parameterType);
    }

    static boolean isLargeObject(Object value) {
        if (value instanceof SqlTypedValue) {
            return true;
        }
        if (!(value instanceof Parameter parameter)) {
            return false;
        }
        // 批处理 LOB 分流和实际绑定遵守同一读取语义，不能对状态型 Parameter 重复取值后改变判型结果。
        Object parameterValue = parameter.getValue();
        Type parameterType = parameter.getType();
        return isCharacterLargeObject(parameterValue, parameterType)
                || isBinaryLargeObject(parameterValue, parameterType);
    }

    private static ByteBuffer binaryContent(Object value, boolean owned) {
        if (owned) {
            return (value instanceof byte[] bytes ? ByteBuffer.wrap(bytes) : (ByteBuffer) value)
                    .asReadOnlyBuffer();
        }
        ByteBuffer source = value instanceof byte[] bytes
                ? ByteBuffer.wrap(bytes)
                : ((ByteBuffer) value).duplicate();
        byte[] snapshot = new byte[source.remaining()];
        source.get(snapshot);
        return ByteBuffer.wrap(snapshot).asReadOnlyBuffer();
    }

    private static ByteBuffer snapshotBinary(Object value) {
        if (value instanceof byte[] bytes) {
            return ByteBuffer.wrap(bytes.clone()).asReadOnlyBuffer();
        }
        return (ByteBuffer) BindableValueSnapshots.immutableValue(value);
    }

    private static boolean isCharacterLargeObject(Object value, Type type) {
        return value instanceof CharSequence
                && (R2dbcType.CLOB.equals(type) || R2dbcType.NCLOB.equals(type));
    }

    private static boolean isBinaryLargeObject(Object value, Type type) {
        return (value instanceof byte[] || value instanceof ByteBuffer) && R2dbcType.BLOB.equals(type);
    }

    private static Parameter stableParameter(Parameter source, Type type, Object value) {
        boolean input = source instanceof Parameter.In;
        boolean output = source instanceof Parameter.Out;
        if (input && output) {
            return new StableInOutParameter(type, value);
        }
        if (input) {
            return new StableInParameter(type, value);
        }
        if (output) {
            return new StableOutParameter(type, value);
        }
        return new StableParameter(type, value);
    }

    private static class StableParameter implements Parameter {

        private final Type type;
        private final Object value;

        private StableParameter(Type type, Object value) {
            this.type = type;
            this.value = value;
        }

        @Override
        public Type getType() {
            return type;
        }

        @Override
        public Object getValue() {
            return value;
        }
    }

    private static final class StableInParameter extends StableParameter implements Parameter.In {

        private StableInParameter(Type type, Object value) {
            super(type, value);
        }
    }

    private static final class StableOutParameter extends StableParameter implements Parameter.Out {

        private StableOutParameter(Type type, Object value) {
            super(type, value);
        }
    }

    private static final class StableInOutParameter extends StableParameter implements Parameter.In, Parameter.Out {

        private StableInOutParameter(Type type, Object value) {
            super(type, value);
        }
    }
}
