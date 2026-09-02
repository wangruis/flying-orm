package com.flying.orm.rdb.reactive;

import com.flying.orm.core.internal.hash.StableDigest;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.codec.SqlTypedValue;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.internal.binding.SqlNullParameter;
import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.Type;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
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
 * <p>该类只保留批量值支持范围和结构规则，计划和数据分别使用 Core 的域隔离稳定编码。</p>
 *
 * @author wangr
 * @date 2026-07-24
 * @version v1.0
 */
final class BatchPayloadHasher {

    private static final int MAX_NESTING_DEPTH = 64;
    private static final StableDigest.Domain PLAN_DOMAIN = StableDigest.domain("batch-plan/v2");
    private static final StableDigest.Domain PAYLOAD_DOMAIN = StableDigest.domain("batch-payload/v1");

    /**
     * 为 SQL 计划生成摘要。计划变了，就不能复用旧回执。
     *
     * @param request 批量请求
     * @return 十六进制摘要
     */
    String hashPlan(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        BatchReceiptDigest digest = BatchReceiptDigest.create(PLAN_DOMAIN);
        digest.text("SQL", safeRequest.sql())
              .integer("PARAM_COUNT", safeRequest.parameterCount())
              .text("MARKERS", safeRequest.bindMarkerStyle().name());
        // 默认 ANY 不写入额外片段；只有显式行数策略才改变计划身份。
        if (safeRequest.rowCountPolicy() != BatchRowCountPolicy.ANY) {
            digest.text("ROW_COUNT_POLICY", safeRequest.rowCountPolicy().name());
        }
        for (Class<?> parameterType : safeRequest.parameterTypes()) {
            digest.text("PARAM_TYPE", parameterType.getName());
        }
        digest.text("MODE", safeRequest.options().mode().name())
              .integer("CHUNK_SIZE", safeRequest.options().chunkSize())
              .integer("CHUNK_BYTES", safeRequest.options().maxBufferedBytes() / safeRequest.options().concurrency())
              .integer("ROW_BYTES", safeRequest.options().maxRowBytes());
        return finish(digest);
    }

    /**
     * 为一批参数行生成摘要。
     *
     * @param rows 参数行
     * @return 十六进制摘要
     */
    String hashRows(List<Object[]> rows) {
        BatchReceiptDigest digest = newPayloadEncoder();
        for (Object[] row : Objects.requireNonNull(rows, "batch rows must not be null")) {
            updateRow(digest, row);
        }
        return finish(digest);
    }

    String hashRowViews(List<ProtectedBatchRows.RowView> rows) {
        BatchReceiptDigest digest = newPayloadEncoder();
        for (ProtectedBatchRows.RowView row : Objects.requireNonNull(
                rows, "batch row views must not be null")) {
            updateRow(digest, row);
        }
        return finish(digest);
    }

    BatchReceiptDigest newPayloadEncoder() {
        return BatchReceiptDigest.create(PAYLOAD_DOMAIN);
    }

    void updateRow(BatchReceiptDigest digest, Object[] row) {
        Object[] safeRow = Objects.requireNonNull(row, "batch row must not be null");
        updateRow(digest, safeRow, safeRow.length);
    }

    void updateRow(BatchReceiptDigest digest, ProtectedBatchRows.RowView row) {
        ProtectedBatchRows.RowView safeRow = Objects.requireNonNull(
                row, "batch row view must not be null");
        Object[] receipt = safeRow.receiptParameters();
        updateRow(digest, receipt == null ? safeRow.row() : receipt, safeRow.parameterCount());
    }

    private void updateRow(BatchReceiptDigest digest, Object[] row, int valueCount) {
        Objects.requireNonNull(digest, "digest must not be null");
        Object[] safeRow = Objects.requireNonNull(row, "batch row must not be null");
        // 行边界是摘要协议的一部分，防止 ["ab", "c"] 与 ["a", "bc"] 产生相同输入流。
        digest.marker("ROW_START");
        IdentityHashMap<Object, Boolean> activeContainers = new IdentityHashMap<>();
        for (int index = 0; index < valueCount; index++) {
            updateValue(digest, safeRow[index], activeContainers, 0);
        }
        digest.marker("ROW_END");
    }

    String finish(BatchReceiptDigest digest) {
        return Objects.requireNonNull(digest, "digest must not be null").finish();
    }

    private void updateValue(BatchReceiptDigest digest,
                             Object value,
                             IdentityHashMap<Object, Boolean> activeContainers,
                             int depth) {
        if (depth > MAX_NESTING_DEPTH) {
            throw new IllegalArgumentException("batch payload nesting exceeds " + MAX_NESTING_DEPTH);
        }
        // 先处理包装器，再按清晰的标量/时间/容器边界分派；具体协议标签集中在对应方法中。
        if (value instanceof SqlNullParameter typedNull) {
            digest.text("SQL_TYPED_NULL", typedNull.javaType().getName());
        } else if (value instanceof SqlTypedValue typedValue) {
            updateSqlTypedValue(digest, typedValue, activeContainers, depth);
        } else if (value instanceof Parameter parameter) {
            updateParameter(digest, parameter, activeContainers, depth);
        } else if (value == null) {
            digest.marker("NULL");
        } else if (value instanceof CharSequence text) {
            digest.text("TEXT", text.toString());
        } else if (value instanceof Boolean bool) {
            digest.bool("BOOLEAN", bool);
        } else if (value instanceof Number number) {
            updateNumber(digest, number);
        } else if (value instanceof UUID uuid) {
            digest.uuid(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        } else if (value instanceof TemporalAccessor temporal) {
            updateTemporal(digest, temporal);
        } else if (value instanceof java.util.Date date) {
            updateDate(digest, date);
        } else if (value instanceof byte[] bytes) {
            digest.bytes("BYTES", bytes);
        } else if (value instanceof ByteBuffer buffer) {
            digest.bytes("BYTE_BUFFER", buffer);
        } else if (value.getClass().isArray()) {
            updateArray(digest, value, activeContainers, depth);
        } else if (value instanceof List<?> values) {
            enterContainer(activeContainers, values);
            try {
                digest.integer("LIST_START", values.size());
                for (Object item : values) {
                    updateValue(digest, item, activeContainers, depth + 1);
                }
                digest.marker("LIST_END");
            } finally {
                activeContainers.remove(values);
            }
        } else {
            // 不稳定的兜底摘要比直接失败更危险，它可能让恢复流程错误复用另一批回执。
            throw unsupported(value);
        }
    }

    private void updateSqlTypedValue(BatchReceiptDigest digest,
                                     SqlTypedValue value,
                                     IdentityHashMap<Object, Boolean> activeContainers,
                                     int depth) {
        enterContainer(activeContainers, value);
        try {
            digest.text("SQL_TYPED_VALUE", value.kind().name());
            updateValue(digest, value.value(), activeContainers, depth + 1);
        } finally {
            activeContainers.remove(value);
        }
    }

    private void updateParameter(BatchReceiptDigest digest,
                                 Parameter parameter,
                                 IdentityHashMap<Object, Boolean> activeContainers,
                                 int depth) {
        enterContainer(activeContainers, parameter);
        try {
            Type type = Objects.requireNonNull(parameter.getType(), "R2DBC parameter type must not be null");
            digest.text("R2DBC_PARAMETER_TYPE", type.getJavaType().getName())
                  .text("R2DBC_PARAMETER_SQL_TYPE", type.getName())
                  .text("R2DBC_PARAMETER_DIRECTION", parameterDirection(parameter));
            updateValue(digest, parameter.getValue(), activeContainers, depth + 1);
        } finally {
            activeContainers.remove(parameter);
        }
    }

    private static String parameterDirection(Parameter parameter) {
        if (parameter instanceof Parameter.In) {
            return parameter instanceof Parameter.Out ? "IN_OUT" : "IN";
        }
        return parameter instanceof Parameter.Out ? "OUT" : "UNSPECIFIED";
    }

    private void updateNumber(BatchReceiptDigest digest, Number number) {
        if (number instanceof Byte value) {
            digest.integer("INT8", value);
        } else if (number instanceof Short value) {
            digest.integer("INT16", value);
        } else if (number instanceof Integer value) {
            digest.integer("INT32", value);
        } else if (number instanceof Long value) {
            digest.integer("INT64", value);
        } else if (number instanceof BigInteger value) {
            digest.bytes("BIG_INTEGER", value.toByteArray());
        } else if (number instanceof BigDecimal value) {
            digest.text("BIG_DECIMAL", value.toPlainString());
        } else if (number instanceof Float value) {
            digest.integer("FLOAT32", Float.floatToIntBits(value));
        } else if (number instanceof Double value) {
            digest.integer("FLOAT64", Double.doubleToLongBits(value));
        } else {
            throw unsupported(number);
        }
    }

    private void updateTemporal(BatchReceiptDigest digest, TemporalAccessor temporal) {
        if (temporal instanceof LocalDate value) {
            digest.text("LOCAL_DATE", value.toString());
        } else if (temporal instanceof LocalTime value) {
            digest.text("LOCAL_TIME", value.toString());
        } else if (temporal instanceof LocalDateTime value) {
            digest.text("LOCAL_DATE_TIME", value.toString());
        } else if (temporal instanceof OffsetDateTime value) {
            digest.text("OFFSET_DATE_TIME", value.toString());
        } else if (temporal instanceof OffsetTime value) {
            digest.text("OFFSET_TIME", value.toString());
        } else if (temporal instanceof ZonedDateTime value) {
            digest.text("ZONED_DATE_TIME", value.toString());
        } else if (temporal instanceof Instant value) {
            digest.text("INSTANT", value.toString());
        } else {
            throw unsupported(temporal);
        }
    }

    private void updateDate(BatchReceiptDigest digest, java.util.Date date) {
        if (date instanceof Timestamp timestamp) {
            Instant instant = timestamp.toInstant();
            digest.marker("SQL_TIMESTAMP")
                  .integer("TIMESTAMP_SECOND", instant.getEpochSecond())
                  .integer("TIMESTAMP_NANO", instant.getNano());
        } else if (date instanceof java.sql.Date sqlDate) {
            // JDBC Date/Time 表示本地业务值，摘要不能依赖 JVM 默认时区。
            digest.text("SQL_DATE", sqlDate.toLocalDate().toString());
        } else if (date instanceof Time time) {
            digest.text("SQL_TIME", time.toLocalTime().toString());
        } else {
            digest.integer("UTIL_DATE", date.getTime());
        }
    }

    private void updateArray(BatchReceiptDigest digest,
                             Object array,
                             IdentityHashMap<Object, Boolean> activeContainers,
                             int depth) {
        // Array 反射同时支持 Object[] 和所有基本类型数组，组件类型也进入摘要以避免跨类型碰撞。
        enterContainer(activeContainers, array);
        try {
            Class<?> componentType = array.getClass().getComponentType();
            int length = Array.getLength(array);
            digest.text("ARRAY_TYPE", componentType.getName())
                  .integer("ARRAY_START", length);
            for (int index = 0; index < length; index++) {
                updateValue(digest, Array.get(array, index), activeContainers, depth + 1);
            }
            digest.marker("ARRAY_END");
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

    private static IllegalArgumentException unsupported(Object value) {
        return new IllegalArgumentException("unsupported batch payload value type: "
                + value.getClass().getName());
    }

}
