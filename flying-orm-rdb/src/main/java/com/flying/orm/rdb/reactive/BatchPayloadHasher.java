package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.codec.SqlTypedValue;
import io.r2dbc.spi.Parameter;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * 用稳定的“类型标签 + 长度 + 值”编码生成 SHA-256 摘要，给批量回执和 UNKNOWN 恢复识别请求身份。
 *
 * <p>摘要必须区分 {@code 1}、{@code 1L} 和文本 {@code "1"}，也必须避免数组或相邻字段拼接后产生边界歧义，
 * 所以每个片段都写入类型和长度。未知对象不使用 {@code toString()} 兜底，因为默认 toString 往往包含地址，
 * 同一批数据重试时并不稳定。</p>
 *
 * <p>该类不保存 MessageDigest；每次请求创建自己的摘要器，可以并发共享实例。</p>
 *
 * @author wangr
 * @date 2026-07-24
 * @version v1.0
 */
final class BatchPayloadHasher {

    private static final int MAX_NESTING_DEPTH = 64;

    /**
     * 为 SQL 计划生成摘要。计划变了，就不能复用旧回执。
     *
     * @param request 批量请求
     * @return 十六进制摘要
     */
    String hashPlan(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        MessageDigest digest = newDigest();
        put(digest, "SQL", safeRequest.sql().getBytes(StandardCharsets.UTF_8));
        put(digest, "PARAM_COUNT", Long.toString(safeRequest.parameterCount()).getBytes(StandardCharsets.UTF_8));
        put(digest, "MARKERS", safeRequest.bindMarkerStyle().name().getBytes(StandardCharsets.UTF_8));
        // 默认 ANY 不能改变旧回执的计划摘要；只有新策略才追加标记。
        if (safeRequest.rowCountPolicy() != BatchRowCountPolicy.ANY) {
            put(digest, "ROW_COUNT_POLICY", safeRequest.rowCountPolicy().name().getBytes(StandardCharsets.UTF_8));
        }
        for (Class<?> parameterType : safeRequest.parameterTypes()) {
            put(digest, "PARAM_TYPE", parameterType.getName().getBytes(StandardCharsets.UTF_8));
        }
        put(digest, "MODE", safeRequest.options().mode().name().getBytes(StandardCharsets.UTF_8));
        put(digest, "CHUNK_SIZE", Long.toString(safeRequest.options().chunkSize()).getBytes(StandardCharsets.UTF_8));
        return finish(digest);
    }

    /**
     * 为一批参数行生成摘要。
     *
     * @param rows 参数行
     * @return 十六进制摘要
     */
    String hashRows(List<Object[]> rows) {
        MessageDigest digest = newDigest();
        for (Object[] row : Objects.requireNonNull(rows, "batch rows must not be null")) {
            updateRow(digest, row);
        }
        return finish(digest);
    }

    MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is not available", error);
        }
    }

    void updateRow(MessageDigest digest, Object[] row) {
        Objects.requireNonNull(digest, "digest must not be null");
        Object[] safeRow = Objects.requireNonNull(row, "batch row must not be null");
        // 行边界是摘要协议的一部分，防止 ["ab", "c"] 与 ["a", "bc"] 产生相同输入流。
        put(digest, "ROW_START", new byte[0]);
        IdentityHashMap<Object, Boolean> activeContainers = new IdentityHashMap<>();
        for (Object value : safeRow) {
            updateValue(digest, value, activeContainers, 0);
        }
        put(digest, "ROW_END", new byte[0]);
    }

    String finish(MessageDigest digest) {
        byte[] bytes = Objects.requireNonNull(digest, "digest must not be null").digest();
        StringBuilder builder = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            builder.append(Character.forDigit((value >> 4) & 0xF, 16));
            builder.append(Character.forDigit(value & 0xF, 16));
        }
        return builder.toString();
    }

    private void updateValue(MessageDigest digest,
                             Object value,
                             IdentityHashMap<Object, Boolean> activeContainers,
                             int depth) {
        if (depth > MAX_NESTING_DEPTH) {
            throw new IllegalArgumentException("batch payload nesting exceeds " + MAX_NESTING_DEPTH);
        }
        // 分支顺序同样属于摘要协议；新增类型时只能追加明确编码，不能改变已有类型的标签和表示。
        if (value instanceof SqlTypedValue typedValue) {
            enterContainer(activeContainers, typedValue);
            try {
                put(digest, "SQL_TYPED_VALUE", typedValue.kind().name().getBytes(StandardCharsets.UTF_8));
                updateValue(digest, typedValue.value(), activeContainers, depth + 1);
            } finally {
                activeContainers.remove(typedValue);
            }
        } else if (value instanceof Parameter parameter) {
            enterContainer(activeContainers, parameter);
            try {
                put(digest, "R2DBC_PARAMETER_TYPE", parameter.getType().getJavaType().getName()
                                                            .getBytes(StandardCharsets.UTF_8));
                updateValue(digest, parameter.getValue(), activeContainers, depth + 1);
            } finally {
                activeContainers.remove(parameter);
            }
        } else if (value == null) {
            put(digest, "NULL", new byte[0]);
        } else if (value instanceof CharSequence text) {
            put(digest, "TEXT", text.toString().getBytes(StandardCharsets.UTF_8));
        } else if (value instanceof Boolean bool) {
            put(digest, "BOOLEAN", new byte[]{(byte) (bool ? 1 : 0)});
        } else if (value instanceof Byte number) {
            put(digest, "INT8", new byte[]{number});
        } else if (value instanceof Short number) {
            put(digest, "INT16", ByteBuffer.allocate(Short.BYTES).putShort(number).array());
        } else if (value instanceof Integer number) {
            put(digest, "INT32", ByteBuffer.allocate(Integer.BYTES).putInt(number).array());
        } else if (value instanceof Long number) {
            put(digest, "INT64", ByteBuffer.allocate(Long.BYTES).putLong(number).array());
        } else if (value instanceof BigInteger number) {
            put(digest, "BIG_INTEGER", number.toByteArray());
        } else if (value instanceof BigDecimal number) {
            put(digest, "BIG_DECIMAL", number.toPlainString().getBytes(StandardCharsets.UTF_8));
        } else if (value instanceof Float number) {
            put(digest, "FLOAT32", Integer.toHexString(Float.floatToIntBits(number))
                                          .getBytes(StandardCharsets.UTF_8));
        } else if (value instanceof Double number) {
            put(digest, "FLOAT64", Long.toHexString(Double.doubleToLongBits(number))
                                       .getBytes(StandardCharsets.UTF_8));
        } else if (value instanceof UUID uuid) {
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
            buffer.putLong(uuid.getMostSignificantBits());
            buffer.putLong(uuid.getLeastSignificantBits());
            put(digest, "UUID", buffer.array());
        } else if (value instanceof LocalDate date) {
            put(digest, "LOCAL_DATE", date.toString().getBytes(StandardCharsets.UTF_8));
        } else if (value instanceof LocalTime time) {
            put(digest, "LOCAL_TIME", time.toString().getBytes(StandardCharsets.UTF_8));
        } else if (value instanceof LocalDateTime dateTime) {
            put(digest, "LOCAL_DATE_TIME", dateTime.toString().getBytes(StandardCharsets.UTF_8));
        } else if (value instanceof OffsetDateTime dateTime) {
            put(digest, "OFFSET_DATE_TIME", dateTime.toString().getBytes(StandardCharsets.UTF_8));
        } else if (value instanceof OffsetTime time) {
            put(digest, "OFFSET_TIME", time.toString().getBytes(StandardCharsets.UTF_8));
        } else if (value instanceof ZonedDateTime dateTime) {
            put(digest, "ZONED_DATE_TIME", dateTime.toString().getBytes(StandardCharsets.UTF_8));
        } else if (value instanceof Instant instant) {
            put(digest, "INSTANT", instant.toString().getBytes(StandardCharsets.UTF_8));
        } else if (value instanceof byte[] bytes) {
            put(digest, "BYTES", bytes);
        } else if (value instanceof ByteBuffer buffer) {
            ByteBuffer copy = buffer.asReadOnlyBuffer();
            byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            put(digest, "BYTE_BUFFER", bytes);
        } else if (value.getClass().isArray()) {
            updateArray(digest, value, activeContainers, depth);
        } else if (value instanceof List<?> values) {
            enterContainer(activeContainers, values);
            try {
                put(digest, "LIST_START", intBytes(values.size()));
                for (Object item : values) {
                    updateValue(digest, item, activeContainers, depth + 1);
                }
                put(digest, "LIST_END", new byte[0]);
            } finally {
                activeContainers.remove(values);
            }
        } else {
            // 不稳定的兜底摘要比直接失败更危险，它可能让恢复流程错误复用另一批回执。
            throw new IllegalArgumentException("unsupported batch payload value type: " + value.getClass().getName());
        }
    }

    private void updateArray(MessageDigest digest,
                             Object array,
                             IdentityHashMap<Object, Boolean> activeContainers,
                             int depth) {
        // Array 反射同时支持 Object[] 和所有基本类型数组，组件类型也进入摘要以避免跨类型碰撞。
        enterContainer(activeContainers, array);
        try {
            Class<?> componentType = array.getClass().getComponentType();
            int length = Array.getLength(array);
            put(digest, "ARRAY_TYPE", componentType.getName().getBytes(StandardCharsets.UTF_8));
            put(digest, "ARRAY_START", intBytes(length));
            for (int index = 0; index < length; index++) {
                updateValue(digest, Array.get(array, index), activeContainers, depth + 1);
            }
            put(digest, "ARRAY_END", new byte[0]);
        } finally {
            activeContainers.remove(array);
        }
    }

    /** 同一个容器可以在不同参数中复用，但不能在自己的子树里再次出现。 */
    private void enterContainer(IdentityHashMap<Object, Boolean> activeContainers, Object container) {
        if (activeContainers.put(container, Boolean.TRUE) != null) {
            throw new IllegalArgumentException("batch payload contains a cyclic value");
        }
    }

    private byte[] intBytes(int value) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
    }

    private void put(MessageDigest digest, String type, byte[] value) {
        // 每段都使用长度前缀，摘要输入具有可逆边界，不依赖任何分隔字符约定。
        byte[] typeBytes = type.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(typeBytes.length).array());
        digest.update(typeBytes);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(value.length).array());
        digest.update(value);
    }
}
