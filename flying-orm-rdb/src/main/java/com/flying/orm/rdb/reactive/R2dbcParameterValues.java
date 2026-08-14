package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.codec.SqlTypedValue;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.R2dbcType;
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
        if (value instanceof SqlTypedValue typedValue) {
            return typedValue.kind() == SqlTypedValue.Kind.BLOB
                    ? Blob.from(Mono.just(binaryContent(typedValue.value())))
                    : Clob.from(Mono.just((CharSequence) typedValue.value()));
        }
        if (isCharacterLargeObject(value)) {
            Parameter parameter = (Parameter) value;
            CharSequence text = (CharSequence) parameter.getValue();
            return Clob.from(Mono.just(text));
        }
        if (isBinaryLargeObject(value)) {
            Object binary = ((Parameter) value).getValue();
            ByteBuffer content = binary instanceof byte[] bytes
                    ? ByteBuffer.wrap(bytes)
                    : ((ByteBuffer) binary).duplicate();
            return Blob.from(Mono.just(content));
        }
        return value;
    }

    static boolean isCharacterLargeObject(Object value) {
        return value instanceof Parameter parameter
                && parameter.getValue() instanceof CharSequence
                && (R2dbcType.CLOB.equals(parameter.getType()) || R2dbcType.NCLOB.equals(parameter.getType()));
    }

    static boolean isBinaryLargeObject(Object value) {
        return value instanceof Parameter parameter
                && (parameter.getValue() instanceof byte[] || parameter.getValue() instanceof ByteBuffer)
                && R2dbcType.BLOB.equals(parameter.getType());
    }

    static boolean isLargeObject(Object value) {
        return value instanceof SqlTypedValue || isCharacterLargeObject(value) || isBinaryLargeObject(value);
    }

    private static ByteBuffer binaryContent(Object value) {
        return value instanceof byte[] bytes ? ByteBuffer.wrap(bytes) : ((ByteBuffer) value).duplicate();
    }
}
