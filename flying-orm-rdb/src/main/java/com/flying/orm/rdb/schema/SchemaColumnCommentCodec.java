package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.core.type.LogicalType;

import java.util.Objects;

/** 把必须随列注释保存的方言结构信息与用户注释合成一个可逆字符串。 */
final class SchemaColumnCommentCodec {

    static final String TIME_MARKER = "[[flying-orm:v1:TIME]]";
    static final String OFFSET_TIME_MARKER = "[[flying-orm:v1:OFFSET_TIME]]";
    static final String COMMENT_ESCAPE = "[[flying-orm:v1:COMMENT]]";
    static final String ORACLE_SEQUENCE_MARKER_PREFIX = "[[flying-orm:v1:SEQUENCE:";

    private SchemaColumnCommentCodec() {
    }

    static String encode(SchemaDialect dialect,
                         DatabaseType dataType,
                         ValueGeneration generation,
                         String comment) {
        SchemaDialect safeDialect = Objects.requireNonNull(dialect, "schema dialect must not be null");
        DatabaseType safeType = Objects.requireNonNull(dataType, "database type must not be null");
        ValueGeneration safeGeneration = Objects.requireNonNull(
                generation, "value generation must not be null");
        String storageComment = encodeLogicalType(safeDialect.generatedValueStyle(), safeType, comment);
        if (safeDialect.generatedValueStyle() != SchemaDialect.GeneratedValueStyle.ORACLE
                || safeGeneration.strategy() != ValueGeneration.Strategy.SEQUENCE) {
            return storageComment;
        }
        String marker = ORACLE_SEQUENCE_MARKER_PREFIX + safeGeneration.sequenceName() + "]]";
        return storageComment == null ? marker : marker + storageComment;
    }

    private static String encodeLogicalType(SchemaDialect.GeneratedValueStyle style,
                                            DatabaseType dataType,
                                            String comment) {
        if (style != SchemaDialect.GeneratedValueStyle.ORACLE
                && style != SchemaDialect.GeneratedValueStyle.MYSQL
                && style != SchemaDialect.GeneratedValueStyle.SQL_SERVER) {
            return comment;
        }
        String marker;
        if (dataType.logicalType() == LogicalType.OFFSET_TIME) {
            marker = OFFSET_TIME_MARKER;
        } else if (style == SchemaDialect.GeneratedValueStyle.ORACLE
                && dataType.logicalType() == LogicalType.TIME) {
            marker = TIME_MARKER;
        } else {
            return escapeReservedComment(comment);
        }
        return comment == null || comment.isEmpty() ? marker : marker + comment;
    }

    /** 用户注释允许使用任意文本；碰到内部协议前缀时只转义一次，读取端会恢复原文。 */
    private static String escapeReservedComment(String comment) {
        if (comment == null || comment.isEmpty()) {
            return comment;
        }
        return comment.startsWith(TIME_MARKER)
                || comment.startsWith(OFFSET_TIME_MARKER)
                || comment.startsWith(COMMENT_ESCAPE)
                || comment.startsWith(ORACLE_SEQUENCE_MARKER_PREFIX)
                ? COMMENT_ESCAPE + comment
                : comment;
    }
}
