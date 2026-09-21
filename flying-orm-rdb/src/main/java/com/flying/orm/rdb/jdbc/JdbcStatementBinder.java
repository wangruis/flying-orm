package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.codec.SqlTypedValue;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.internal.binding.SqlNullParameter;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 把共享 SQL 请求中的 Java 参数交给 JDBC。
 *
 * <p>普通值直接走 {@link PreparedStatement#setObject(int, Object)}；只有 codec 明确标记的 LOB 才使用流式
 * 绑定，避免 Oracle 等驱动先按 VARCHAR/RAW 的长度限制处理。类中没有可变状态，可以被多个执行器并发复用。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
final class JdbcStatementBinder {

    /** 只在显式空值路径读取；未知驱动扩展类型交给 JDBC 的 OTHER 语义。 */
    private static final Map<Class<?>, Integer> NULL_SQL_TYPES = Map.ofEntries(
            Map.entry(Boolean.class, Types.BOOLEAN),
            Map.entry(Byte.class, Types.TINYINT),
            Map.entry(Short.class, Types.SMALLINT),
            Map.entry(Integer.class, Types.INTEGER),
            Map.entry(Long.class, Types.BIGINT),
            Map.entry(BigInteger.class, Types.NUMERIC),
            Map.entry(BigDecimal.class, Types.DECIMAL),
            Map.entry(Float.class, Types.REAL),
            Map.entry(Double.class, Types.DOUBLE),
            Map.entry(Character.class, Types.CHAR),
            Map.entry(String.class, Types.VARCHAR),
            Map.entry(byte[].class, Types.VARBINARY),
            Map.entry(ByteBuffer.class, Types.VARBINARY),
            Map.entry(LocalDate.class, Types.DATE),
            Map.entry(LocalTime.class, Types.TIME),
            Map.entry(OffsetTime.class, Types.TIME_WITH_TIMEZONE),
            Map.entry(LocalDateTime.class, Types.TIMESTAMP),
            Map.entry(Instant.class, Types.TIMESTAMP_WITH_TIMEZONE),
            Map.entry(OffsetDateTime.class, Types.TIMESTAMP_WITH_TIMEZONE));

    private JdbcStatementBinder() {
    }

    static void bind(PreparedStatement statement, List<Object> parameters) throws SQLException {
        PreparedStatement safeStatement = Objects.requireNonNull(statement, "jdbc statement must not be null");
        List<Object> safeParameters = Objects.requireNonNull(parameters, "sql parameters must not be null");
        for (int index = 0; index < safeParameters.size(); index++) {
            bindValue(safeStatement, index + 1, safeParameters.get(index));
        }
    }

    static void bindOwned(PreparedStatement statement, ProtectedBatchRows.RowView rowView) throws SQLException {
        PreparedStatement safeStatement = Objects.requireNonNull(statement, "jdbc statement must not be null");
        ProtectedBatchRows.RowView safeRow = Objects.requireNonNull(
                rowView, "protected batch row view must not be null");
        for (int index = 0; index < safeRow.parameterCount(); index++) {
            bindValue(safeStatement, index + 1, safeRow.row()[index]);
        }
    }

    private static void bindValue(PreparedStatement statement, int index, Object value) throws SQLException {
        if (value instanceof SqlNullParameter typedNull) {
            statement.setNull(index, NULL_SQL_TYPES.getOrDefault(typedNull.javaType(), Types.OTHER));
            return;
        }
        if (!(value instanceof SqlTypedValue typedValue)) {
            if (value instanceof ByteBuffer buffer) {
                ByteBuffer readable = buffer.duplicate();
                statement.setBinaryStream(index, new ByteBufferInputStream(readable), readable.remaining());
            } else {
                statement.setObject(index, value);
            }
            return;
        }
        switch (typedValue.kind()) {
            case BLOB -> bindBlob(statement, index, typedValue.value());
            case CLOB -> {
                String text = typedValue.value().toString();
                statement.setCharacterStream(index, new StringReader(text), text.length());
            }
            case NCLOB -> {
                String text = typedValue.value().toString();
                statement.setNCharacterStream(index, new StringReader(text), text.length());
            }
        }
    }

    private static void bindBlob(PreparedStatement statement, int index, Object value) throws SQLException {
        if (value instanceof byte[] bytes) {
            statement.setBinaryStream(index, new ByteArrayInputStream(bytes), bytes.length);
            return;
        }
        ByteBuffer readable = ((ByteBuffer) value).duplicate();
        statement.setBinaryStream(index, new ByteBufferInputStream(readable), readable.remaining());
    }

    /** 直接读取 duplicate 后的 ByteBuffer，不为直接内存再复制一份 byte[]。 */
    private static final class ByteBufferInputStream extends java.io.InputStream {

        private final ByteBuffer buffer;

        private ByteBufferInputStream(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public int read() {
            return buffer.hasRemaining() ? buffer.get() & 0xff : -1;
        }

        @Override
        public int read(byte[] target, int offset, int length) {
            Objects.checkFromIndexSize(offset, length, target.length);
            if (length == 0) {
                return 0;
            }
            if (!buffer.hasRemaining()) {
                return -1;
            }
            int readable = Math.min(length, buffer.remaining());
            buffer.get(target, offset, readable);
            return readable;
        }
    }
}
