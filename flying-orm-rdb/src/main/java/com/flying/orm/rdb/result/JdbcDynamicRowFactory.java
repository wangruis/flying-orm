package com.flying.orm.rdb.result;

import static com.flying.orm.core.internal.error.ThrowableGraph.addSuppressedIfAcyclic;
import static com.flying.orm.core.internal.error.ThrowableGraph.findVirtualMachineError;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlLargeObjectLimitExceededException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 把一个 JDBC {@link ResultSet} 当前行压缩成 {@link DynamicRow}。
 *
 * <p>工厂创建时只读取一次列元数据，后续每行只按下标取值，因此同一结果集内的行会共享列布局。这个对象持有
 * JDBC 游标，只能由正在消费该 ResultSet 的一个线程使用，不能跨线程共享，也不能复用到另一个 ResultSet。
 * Blob、Clob、SQLXML 和 Array 等驱动临时资源会在当前行内物化并释放。</p>
 *
 * @author wangr
 * @date 2026-08-13
 * @version v1.0
 */
public final class JdbcDynamicRowFactory {

    private static final int BUFFER_SIZE = 8 * 1024;

    private final ResultSet resultSet;
    private final RowLayout layout;
    private final SqlExecutionOptions options;
    private final boolean[] limitedColumns;

    private JdbcDynamicRowFactory(ResultSet resultSet, RowLayout layout,
                                  SqlExecutionOptions options, boolean[] limitedColumns) {
        this.resultSet = resultSet;
        this.layout = layout;
        this.options = options;
        this.limitedColumns = limitedColumns;
    }

    /**
     * 为一个已经定位到可读取行的 ResultSet 创建工厂。调用方仍负责 {@code next()} 和关闭 ResultSet。
     */
    public static JdbcDynamicRowFactory from(ResultSet resultSet, SqlExecutionOptions options) throws SQLException {
        ResultSet safeResultSet = Objects.requireNonNull(resultSet, "jdbc result set must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        ResultSetMetaData metadata = Objects.requireNonNull(
                safeResultSet.getMetaData(), "jdbc result set metadata must not be null");
        int columnCount = metadata.getColumnCount();
        List<String> names = new ArrayList<>(columnCount);
        boolean[] limited = null;
        boolean hasLimits = safeOptions.maxLargeObjectBytes() > 0 || safeOptions.maxLargeObjectChars() > 0;
        for (int index = 1; index <= columnCount; index++) {
            String label = metadata.getColumnLabel(index);
            names.add(label == null || label.isBlank() ? metadata.getColumnName(index) : label);
            // 驱动可能已经把 LOB 物化为 byte[]/String；类型只在结果布局建立时读取一次。
            // 明确的普通标量列不消耗 LOB 额度，未知类型则保留原来的保守大小边界。
            if (hasLimits && limitMaterializedColumn(metadata.getColumnType(index))) {
                if (limited == null) {
                    limited = new boolean[columnCount];
                }
                limited[index - 1] = true;
            }
        }
        return new JdbcDynamicRowFactory(safeResultSet, RowLayout.of(names), safeOptions, limited);
    }

    /**
     * 读取 ResultSet 当前行。这里不会移动游标，执行器可以在自己的 while (resultSet.next()) 循环中调用它。
     */
    public DynamicRow readCurrentRow() throws SQLException {
        Object[] values = new Object[layout.size()];
        for (int index = 0; index < values.length; index++) {
            values[index] = materialize(resultSet.getObject(index + 1),
                    limitedColumns != null && limitedColumns[index]);
        }
        return DynamicRow.owned(layout, values);
    }

    private static boolean limitMaterializedColumn(int jdbcType) {
        return switch (jdbcType) {
            case Types.BLOB, Types.CLOB, Types.NCLOB, Types.SQLXML,
                    Types.LONGVARCHAR, Types.LONGNVARCHAR, Types.LONGVARBINARY,
                    Types.OTHER, Types.JAVA_OBJECT, Types.NULL -> true;
            default -> false;
        };
    }

    private Object materialize(Object value, boolean limitMaterialized) throws SQLException {
        if (value == null) {
            return null;
        }
        Object temporalValue = JdbcTemporalValueAdapter.materialize(value, resultSet);
        if (temporalValue != value) {
            return temporalValue;
        }
        if (value instanceof SQLXML sqlXml) {
            return readSqlXml(sqlXml);
        }
        if (value instanceof Array array) {
            return readArray(array);
        }
        if (value instanceof Blob blob) {
            return readBlob(blob);
        }
        if (value instanceof Clob clob) {
            return readClob(clob);
        }
        if (limitMaterialized && value instanceof byte[] bytes) {
            requireWithinLimit(SqlLargeObjectLimitExceededException.Kind.BINARY,
                               options.maxLargeObjectBytes(), bytes.length);
        } else if (limitMaterialized && value instanceof CharSequence text) {
            requireWithinLimit(SqlLargeObjectLimitExceededException.Kind.CHARACTER,
                               options.maxLargeObjectChars(), text.length());
        }
        return value;
    }

    /** PostgreSQL 等 JDBC 驱动用临时 Array 句柄返回数组列，离开当前行前必须物化并释放。 */
    private static Object readArray(Array array) throws SQLException {
        return materializeAndRelease(array::getArray, array::free);
    }

    private byte[] readBlob(Blob blob) throws SQLException {
        return materializeAndRelease(() -> {
            try (InputStream input = blob.getBinaryStream(); ByteArrayOutputStream output = new ByteArrayOutputStream()) {
                byte[] buffer = new byte[BUFFER_SIZE];
                long size = 0;
                for (int read; (read = input.read(buffer)) >= 0; ) {
                    size = Math.addExact(size, read);
                    requireWithinLimit(SqlLargeObjectLimitExceededException.Kind.BINARY,
                                       options.maxLargeObjectBytes(), size);
                    output.write(buffer, 0, read);
                }
                return output.toByteArray();
            } catch (IOException error) {
                throw new SQLException("failed to materialize JDBC Blob", error);
            }
        }, blob::free);
    }

    private String readClob(Clob clob) throws SQLException {
        return materializeAndRelease(() -> {
            try (Reader reader = clob.getCharacterStream()) {
                return readCharacters(reader);
            } catch (IOException error) {
                throw new SQLException("failed to materialize JDBC Clob", error);
            }
        }, clob::free);
    }

    /** SQLXML 通常只是事务期有效的逻辑指针，必须在当前行内按字符上限物化并释放。 */
    private String readSqlXml(SQLXML sqlXml) throws SQLException {
        return materializeAndRelease(() -> {
            try (Reader reader = sqlXml.getCharacterStream()) {
                return readCharacters(reader);
            } catch (IOException error) {
                throw new SQLException("failed to materialize JDBC SQLXML", error);
            }
        }, sqlXml::free);
    }

    private String readCharacters(Reader reader) throws IOException {
        char[] buffer = new char[BUFFER_SIZE];
        StringBuilder output = new StringBuilder();
        long size = 0;
        for (int read; (read = reader.read(buffer)) >= 0; ) {
            size = Math.addExact(size, read);
            requireWithinLimit(SqlLargeObjectLimitExceededException.Kind.CHARACTER,
                               options.maxLargeObjectChars(), size);
            output.append(buffer, 0, read);
        }
        return output.toString();
    }

    /**
     * 物化驱动临时资源并在同一行内释放；主失败与释放失败同时出现时保持 VME-first 且不制造异常图环。
     */
    private static <T> T materializeAndRelease(JdbcResourceSupplier<T> supplier,
                                                JdbcResourceRelease release) throws SQLException {
        Throwable primary = null;
        try {
            return supplier.get();
        } catch (SQLException | RuntimeException | Error error) {
            primary = error;
            throw error;
        } finally {
            try {
                release.run();
            } catch (SQLException | RuntimeException | Error cleanup) {
                VirtualMachineError primaryFatal = findVirtualMachineError(primary);
                VirtualMachineError cleanupFatal = findVirtualMachineError(cleanup);
                if (primaryFatal != null) {
                    addSuppressedIfAcyclic(primaryFatal, cleanup);
                    throw primaryFatal;
                }
                if (cleanupFatal != null) {
                    addSuppressedIfAcyclic(cleanupFatal, primary);
                    throw cleanupFatal;
                }
                if (primary == null) {
                    throw cleanup;
                }
                addSuppressedIfAcyclic(primary, cleanup);
            }
        }
    }

    private static void requireWithinLimit(SqlLargeObjectLimitExceededException.Kind kind,
                                           long maxSize,
                                           long actualSize) {
        if (maxSize > 0 && actualSize > maxSize) {
            throw new SqlLargeObjectLimitExceededException(kind, maxSize, actualSize);
        }
    }

    @FunctionalInterface
    private interface JdbcResourceSupplier<T> {

        T get() throws SQLException;
    }

    @FunctionalInterface
    private interface JdbcResourceRelease {

        void run() throws SQLException;
    }

}
