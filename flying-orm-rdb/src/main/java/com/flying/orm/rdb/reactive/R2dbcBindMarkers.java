package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlIdentifiers;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.internal.dialect.DatabaseProduct;
import com.flying.orm.rdb.internal.plan.SqlExecutionStatements;
import com.flying.orm.rdb.internal.plan.SqlStatementCompiler;
import io.r2dbc.spi.ConnectionFactory;

import java.util.Locale;
import java.util.Objects;

/**
 * 把渲染层统一使用的问号占位符换成具体 R2DBC 驱动认识的写法。
 * SQL AST 和参数顺序不需要知道驱动差异，执行层只在真正创建 Statement 前转换一次。
 *
 * <p>AUTO 风格的 SQL 必须来自 flying-orm 渲染器，问号都代表参数；调用方传入数据库原生 SQL 时应声明
 * NATIVE，避免把字符串常量里的问号误认为占位符。实例只保存枚举格式，可并发共享。</p>
 */
final class R2dbcBindMarkers {

    private final String databaseProductName;
    private final DatabaseProduct databaseProduct;

    private R2dbcBindMarkers(String databaseProductName) {
        this.databaseProductName = databaseProductName;
        this.databaseProduct = DatabaseProduct.detect(databaseProductName);
    }

    static R2dbcBindMarkers from(ConnectionFactory connectionFactory) {
        // ConnectionFactory metadata 是 R2DBC SPI 在不取连接时识别驱动的标准入口。
        String databaseName = Objects.requireNonNull(connectionFactory.getMetadata(),
                                                     "connection factory metadata must not be null").getName();
        return new R2dbcBindMarkers(databaseName);
    }

    /** 返回本执行器已按真实产品复验并编译的传输 SQL。 */
    String adapt(SqlRequest request) {
        return SqlExecutionStatements.r2dbc(
                Objects.requireNonNull(request, "sql request must not be null"),
                databaseProductName);
    }

    /** 在批量订阅边界复验并编译一次，所有分片复用同一传输 SQL。 */
    String adapt(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(
                request, "batch write request must not be null");
        return SqlExecutionStatements.r2dbc(safeRequest.statement(), databaseProductName);
    }

    /**
     * 把已经通过结构校验的普通或 schema-qualified 标识符按驱动规则逐段引用。
     * PostgreSQL、Oracle 和 H2 先遵守未引用名称的大小写折叠，避免给默认回执表增加大小写语义变化。
     */
    String identifier(String value) {
        String safe = SqlIdentifiers.requireIdentifier(value, "R2DBC SQL identifier");
        String[] segments = safe.split("\\.", -1);
        StringBuilder result = new StringBuilder(safe.length() + segments.length * 2);
        for (int index = 0; index < segments.length; index++) {
            if (index > 0) {
                result.append('.');
            }
            appendIdentifier(result, segments[index]);
        }
        return result.toString();
    }

    private void appendIdentifier(StringBuilder result, String segment) {
        if (databaseProduct == DatabaseProduct.POSTGRESQL) {
            result.append('"').append(segment.toLowerCase(Locale.ROOT)).append('"');
        } else if (databaseProduct == DatabaseProduct.ORACLE || databaseProduct == DatabaseProduct.H2) {
            result.append('"').append(segment.toUpperCase(Locale.ROOT)).append('"');
        } else if (databaseProduct == DatabaseProduct.MYSQL) {
            result.append('`').append(segment).append('`');
        } else if (databaseProduct == DatabaseProduct.SQL_SERVER) {
            result.append('[').append(segment).append(']');
        } else {
            result.append(segment);
        }
    }

    String adapt(String sql, int parameterCount, SqlBindMarkerStyle markerStyle) {
        return SqlExecutionStatements.r2dbc(
                SqlStatementCompiler.compile(sql, parameterCount, markerStyle, databaseProductName),
                databaseProductName);
    }

}
