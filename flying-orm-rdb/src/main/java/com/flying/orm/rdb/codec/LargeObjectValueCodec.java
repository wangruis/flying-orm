package com.flying.orm.rdb.codec;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionTimeoutException;
import com.flying.orm.rdb.execution.SqlLargeObjectLimitExceededException;
import com.flying.orm.rdb.internal.ReactiveTimeouts;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * BLOB/CLOB 的字段感知转换。普通 Java 值走同步快路径，R2DBC Blob/Clob 只通过响应式流读取，
 * 不会为了兼容同步调用而在这里 block。
 *
 * <p>物化过程按 chunk 累计并在追加前检查单字段大小上限，超限立即取消读取并抛稳定异常。Blob/Clob 类型与
 * 动态字段不匹配时先 discard 驱动句柄再报错，避免连接上的 LOB 资源泄漏。</p>
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public final class LargeObjectValueCodec {

    private static final ValueCodecRegistry VALUE_CODECS = ValueCodecRegistry.standard();

    private LargeObjectValueCodec() {
    }

    public static boolean isLargeObjectDataType(String dataType) {
        return isBinaryDataType(dataType) || isTextDataType(dataType);
    }

    public static boolean isBinaryDataType(String dataType) {
        return switch (baseType(dataType)) {
            case "BLOB", "BINARY", "VARBINARY", "LONGBLOB", "BYTEA", "RAW", "PROTECTED_BINARY" -> true;
            default -> false;
        };
    }

    public static boolean isTextDataType(String dataType) {
        return switch (baseType(dataType)) {
            case "CLOB", "NCLOB", "TEXT", "LONGTEXT" -> true;
            default -> false;
        };
    }

    /**
     * 写库前只做类型收口，不复制 byte[] 或 ByteBuffer，批量大字段不会平白多一份内存。
     */
    public static Object write(Object value, String dataType) {
        return write(value, dataType, "");
    }

    /**
     * 按数据库驱动需要准备 LOB 参数。Oracle 不能只看目标列是 CLOB/BLOB：如果仍把长文本当普通
     * String 绑定，驱动会先套用 VARCHAR2 的长度上限，大文本还没写到 CLOB 列就会失败。因此这里
     * 为 Oracle 明确带上 R2DBC LOB 类型；其他数据库继续返回原始 Java 值，不改变已经验证过的绑定路径。
     */
    public static Object write(Object value, String dataType, String dialectName) {
        if (value == null) {
            return null;
        }
        if (isBinaryDataType(dataType)) {
            Object binaryValue;
            if (value instanceof byte[] bytes) {
                binaryValue = bytes;
            } else if (value instanceof Byte[] boxed) {
                binaryValue = VALUE_CODECS.write(boxed);
            } else if (value instanceof ByteBuffer buffer) {
                // duplicate 不复制大字段内容，也不会让后续绑定移动调用方传入 buffer 的 position。
                binaryValue = buffer.duplicate();
            } else {
                throw new IllegalArgumentException("binary large object must be byte[] or ByteBuffer");
            }
            if (!"oracle".equalsIgnoreCase(dialectName)) {
                return binaryValue;
            }
            // Oracle MERGE 的 USING 子查询没有目标列上下文，大 byte[] 会被误判成普通 RAW/VARCHAR 值。
            // 先保留可稳定哈希的参数值，执行层会在 bind 前把它变成真正的响应式 Blob。
            return new SqlTypedValue(SqlTypedValue.Kind.BLOB, binaryValue);
        }
        if (isTextDataType(dataType)) {
            if (value instanceof CharSequence || value instanceof char[]) {
                Object text = VALUE_CODECS.write(value);
                if (!"oracle".equalsIgnoreCase(dialectName)) {
                    return text;
                }
                SqlTypedValue.Kind kind = "NCLOB".equals(baseType(dataType))
                        ? SqlTypedValue.Kind.NCLOB
                        : SqlTypedValue.Kind.CLOB;
                return new SqlTypedValue(kind, text);
            }
            throw new IllegalArgumentException("character large object must be CharSequence or char[]");
        }
        return value;
    }

    /**
     * 动态表单没有 Java 字段类型可参考，所以二进制统一返回 byte[]，文本统一返回 String。
     */
    public static Object read(Object value, String dataType) {
        if (value == null) {
            return null;
        }
        if (isBinaryDataType(dataType)) {
            return VALUE_CODECS.read(value, byte[].class);
        }
        if (isTextDataType(dataType)) {
            return VALUE_CODECS.read(value, String.class);
        }
        return value;
    }

    /**
     * 驱动返回 Blob/Clob 句柄时只订阅一次内容流并异步物化。大小上限按单个字段计算，0 表示不限。
     * 类型对不上时不会订阅内容流，而是 discard 这个句柄，把驱动资源还回去。
     */
    public static Mono<Object> readReactive(Object value,
                                            String dataType,
                                            SqlExecutionOptions options) {
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options,
                                                                 "sql execution options must not be null");
        Mono<Object> decoded;
        if (value instanceof Blob blob) {
            if (!isBinaryDataType(dataType)) {
                return Mono.from(blob.discard())
                           .then(Mono.error(new IllegalArgumentException("Blob value requires a binary field")));
            }
            decoded = materialize(blob, safeOptions.maxLargeObjectBytes()).cast(Object.class);
        } else if (value instanceof Clob clob) {
            if (!isTextDataType(dataType)) {
                return Mono.from(clob.discard())
                           .then(Mono.error(new IllegalArgumentException("Clob value requires a text field")));
            }
            decoded = materialize(clob, safeOptions.maxLargeObjectChars()).cast(Object.class);
        } else {
            decoded = Mono.fromSupplier(() -> readMaterialized(value, dataType, safeOptions));
        }
        if (safeOptions.timeout().isZero()) {
            return decoded;
        }
        return decoded.timeout(ReactiveTimeouts.duration(safeOptions.timeout()))
                      .onErrorMap(TimeoutException.class,
                                  error -> new SqlExecutionTimeoutException(safeOptions.timeout(), error));
    }

    private static Mono<byte[]> materialize(Blob blob, long maxBytes) {
        // reduceWith 为每次订阅创建独立 accumulator，同一个 codec 可并发处理多个 LOB。
        return Flux.from(blob.stream())
                   .reduceWith(() -> new BinaryAccumulator(maxBytes), BinaryAccumulator::append)
                   .map(BinaryAccumulator::value);
    }

    private static Mono<String> materialize(Clob clob, long maxChars) {
        return Flux.from(clob.stream())
                   .reduceWith(() -> new CharacterAccumulator(maxChars), CharacterAccumulator::append)
                   .map(CharacterAccumulator::value);
    }

    /**
     * 有些驱动直接给 byte[]、ByteBuffer 或 String。先看大小再转换，不能复制完一大块内存才报超限。
     */
    private static Object readMaterialized(Object value,
                                           String dataType,
                                           SqlExecutionOptions options) {
        if (value != null && isBinaryDataType(dataType)) {
            long size = switch (value) {
                case byte[] bytes -> bytes.length;
                case Byte[] bytes -> bytes.length;
                case ByteBuffer buffer -> buffer.remaining();
                default -> 0;
            };
            requireWithinLimit(SqlLargeObjectLimitExceededException.Kind.BINARY,
                               options.maxLargeObjectBytes(),
                               size);
        } else if (value != null && isTextDataType(dataType)) {
            long size = switch (value) {
                case CharSequence text -> text.length();
                case char[] characters -> characters.length;
                default -> 0;
            };
            requireWithinLimit(SqlLargeObjectLimitExceededException.Kind.CHARACTER,
                               options.maxLargeObjectChars(),
                               size);
        }
        return read(value, dataType);
    }

    private static String baseType(String dataType) {
        String type = Objects.requireNonNull(dataType, "large object data type must not be null")
                             .trim()
                             .toUpperCase(Locale.ROOT);
        int arguments = type.indexOf('(');
        return arguments < 0 ? type : type.substring(0, arguments).trim();
    }

    private static final class BinaryAccumulator {

        private final long maxBytes;

        private final ByteArrayOutputStream output = new ByteArrayOutputStream();

        private long size;

        private BinaryAccumulator(long maxBytes) {
            this.maxBytes = maxBytes;
        }

        private BinaryAccumulator append(ByteBuffer chunk) {
            // duplicate 后读取，不移动驱动提供 buffer 的 position。
            ByteBuffer readable = Objects.requireNonNull(chunk, "Blob chunk must not be null").duplicate();
            long nextSize = Math.addExact(size, readable.remaining());
            requireWithinLimit(SqlLargeObjectLimitExceededException.Kind.BINARY, maxBytes, nextSize);
            if (readable.hasArray()) {
                output.write(readable.array(),
                             readable.arrayOffset() + readable.position(),
                             readable.remaining());
            } else {
                byte[] bytes = new byte[readable.remaining()];
                readable.get(bytes);
                output.writeBytes(bytes);
            }
            size = nextSize;
            return this;
        }

        private byte[] value() {
            return output.toByteArray();
        }
    }

    private static final class CharacterAccumulator {

        private final long maxChars;

        private final StringBuilder output = new StringBuilder();

        private long size;

        private CharacterAccumulator(long maxChars) {
            this.maxChars = maxChars;
        }

        private CharacterAccumulator append(CharSequence chunk) {
            CharSequence text = Objects.requireNonNull(chunk, "Clob chunk must not be null");
            long nextSize = Math.addExact(size, text.length());
            requireWithinLimit(SqlLargeObjectLimitExceededException.Kind.CHARACTER, maxChars, nextSize);
            output.append(text);
            size = nextSize;
            return this;
        }

        private String value() {
            return output.toString();
        }
    }

    private static void requireWithinLimit(SqlLargeObjectLimitExceededException.Kind kind,
                                           long maxSize,
                                           long actualSize) {
        if (maxSize > 0 && actualSize > maxSize) {
            throw new SqlLargeObjectLimitExceededException(kind, maxSize, actualSize);
        }
    }
}
