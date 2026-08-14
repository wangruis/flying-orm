package com.flying.orm.rdb.result;

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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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

    private JdbcDynamicRowFactory(ResultSet resultSet, RowLayout layout, SqlExecutionOptions options) {
        this.resultSet = resultSet;
        this.layout = layout;
        this.options = options;
    }

    /**
     * 为一个已经定位到可读取行的 ResultSet 创建工厂。调用方仍负责 {@code next()} 和关闭 ResultSet。
     */
    public static JdbcDynamicRowFactory from(ResultSet resultSet, SqlExecutionOptions options) throws SQLException {
        ResultSet safeResultSet = Objects.requireNonNull(resultSet, "jdbc result set must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        return new JdbcDynamicRowFactory(safeResultSet, readLayout(safeResultSet.getMetaData()), safeOptions);
    }

    /**
     * 读取 ResultSet 当前行。这里不会移动游标，执行器可以在自己的 while (resultSet.next()) 循环中调用它。
     */
    public DynamicRow readCurrentRow() throws SQLException {
        Object[] values = new Object[layout.size()];
        for (int index = 0; index < values.length; index++) {
            values[index] = materialize(resultSet.getObject(index + 1));
        }
        return DynamicRow.owned(layout, values);
    }

    private static RowLayout readLayout(ResultSetMetaData metadata) throws SQLException {
        ResultSetMetaData safeMetadata = Objects.requireNonNull(metadata, "jdbc result set metadata must not be null");
        int columnCount = safeMetadata.getColumnCount();
        List<String> names = new ArrayList<>(columnCount);
        for (int index = 1; index <= columnCount; index++) {
            String label = safeMetadata.getColumnLabel(index);
            names.add(label == null || label.isBlank() ? safeMetadata.getColumnName(index) : label);
        }
        return RowLayout.of(names);
    }

    private Object materialize(Object value) throws SQLException {
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
        if (value instanceof byte[] bytes) {
            requireWithinLimit(SqlLargeObjectLimitExceededException.Kind.BINARY,
                               options.maxLargeObjectBytes(), bytes.length);
        } else if (value instanceof CharSequence text) {
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

    private static VirtualMachineError findVirtualMachineError(Throwable failure) {
        if (failure == null) {
            return null;
        }
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(failure);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current instanceof VirtualMachineError fatal) {
                return fatal;
            }
            if (current.getCause() != null) {
                pending.addFirst(current.getCause());
            }
            Collections.addAll(pending, current.getSuppressed());
        }
        return null;
    }

    private static void addSuppressedIfAcyclic(Throwable primary, Throwable secondary) {
        if (primary != null && secondary != null && primary != secondary
                && !reaches(primary, secondary) && !reaches(secondary, primary)) {
            primary.addSuppressed(secondary);
        }
    }

    private static boolean reaches(Throwable start, Throwable target) {
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(start);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (current == target) {
                return true;
            }
            if (!visited.add(current)) {
                continue;
            }
            if (current.getCause() != null) {
                pending.add(current.getCause());
            }
            Collections.addAll(pending, current.getSuppressed());
        }
        return false;
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
