package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.result.JdbcDynamicRowFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Objects;

/**
 * 读取 JDBC 单次写入返回的主键。
 *
 * <p>批量主键回填最怕的是驱动少给、多给或给了空值后仍被当作成功。本类把这些情况
 * 统一在回调前拦住；结果集总会在当前行处理完后关闭，不把驱动游标带到下一个输入项。</p>
 */
final class JdbcBatchGeneratedKeyReader {

    private JdbcBatchGeneratedKeyReader() {
    }

    static DynamicRow readOne(ResultSet resultSet,
                              String expectedColumn,
                              SqlExecutionOptions options) throws SQLException {
        if (resultSet == null) {
            throw new SQLException("jdbc driver did not return generated keys", "HY000");
        }
        try (ResultSet keys = resultSet) {
            if (!keys.next()) {
                throw new SQLException("jdbc driver did not return generated key", "HY000");
            }
            DynamicRow row = JdbcDynamicRowFactory.from(keys, options).readCurrentRow();
            requireNonNullKey(row, expectedColumn);
            if (keys.next()) {
                throw new SQLException("jdbc driver returned more than one generated key for one batch row", "HY000");
            }
            return row;
        }
    }

    private static void requireNonNullKey(DynamicRow row, String expectedColumn) throws SQLException {
        DynamicRow safeRow = Objects.requireNonNull(row, "jdbc generated key row must not be null");
        String expected = normalize(expectedColumn);
        Object value = null;
        int matches = 0;
        for (int index = 0; index < safeRow.columnCount(); index++) {
            if (normalize(safeRow.columnName(index)).equals(expected)) {
                value = safeRow.value(index);
                matches++;
            }
        }
        if (matches == 0 && safeRow.columnCount() == 1) {
            value = safeRow.value(0);
            matches = 1;
        }
        if (matches != 1 || value == null) {
            throw new SQLException("jdbc driver returned an invalid generated key", "HY000");
        }
    }

    private static String normalize(String column) {
        return Objects.requireNonNull(column, "generated key column must not be null")
                      .toLowerCase(Locale.ROOT);
    }
}
