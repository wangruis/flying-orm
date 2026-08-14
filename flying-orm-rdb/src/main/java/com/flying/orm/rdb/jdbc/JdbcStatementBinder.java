package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.codec.SqlTypedValue;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
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

    private JdbcStatementBinder() {
    }

    static void bind(PreparedStatement statement, List<Object> parameters) throws SQLException {
        PreparedStatement safeStatement = Objects.requireNonNull(statement, "jdbc statement must not be null");
        List<Object> safeParameters = Objects.requireNonNull(parameters, "sql parameters must not be null");
        for (int index = 0; index < safeParameters.size(); index++) {
            bindValue(safeStatement, index + 1, safeParameters.get(index));
        }
    }

    private static void bindValue(PreparedStatement statement, int index, Object value) throws SQLException {
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
