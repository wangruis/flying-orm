package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.internal.binding.SqlNullParameter;
import com.flying.orm.rdb.internal.template.SqlLexicalScanner;

import java.util.List;

/**
 * 把已有结构化观测事件转换成一行可读日志。
 *
 * <p>这个类只在日志 observer 被调用时工作。SQL 和参数开关关闭时，不读取 SQL 展示文本，
 * 也不遍历参数集合；长度限制在拼接过程中生效，不先生成一条可能很大的临时字符串。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
final class SqlExecutionLogFormatter {

    private SqlExecutionLogFormatter() {
    }

    static String sql(SqlExecutionObservation observation,
                      SqlExecutionLogOptions options,
                      SqlExecutionLogSelection selection,
                      List<Object> parameters,
                      SqlTransactionSource transactionSource) {
        StringBuilder message = new StringBuilder(Math.min(options.maxMessageLength(), 256));
        append(message, options, "kind=SQL");
        append(message, options, "backend=" + observation.backend());
        append(message, options, "statementType=" + observation.statementType());
        append(message, options, "operation=" + observation.operation());
        append(message, options, "status=" + observation.status());
        if (observation.failureCategory() != SqlFailureCategory.NONE) {
            append(message, options, "failureCategory=" + observation.failureCategory());
        }
        if (selection.includeDuration()) {
            append(message, options, "durationNanos=" + observation.durationNanos());
        }
        append(message, options, "parameterCount=" + observation.parameterCount());
        if (observation.operation() == SqlExecutionOperation.QUERY && selection.includeReturnedRows()) {
            append(message, options, "returnedRows=" + observation.rows());
        } else if (observation.operation() != SqlExecutionOperation.QUERY && selection.includeAffectedRows()) {
            append(message, options, "affectedRows=" + observation.rows());
        }
        append(message, options, "transactionSource=" + transactionSource);
        if (options.includeSql()) {
            append(message, options, "sql=\"" + maskSql(observation.sql(), options.maxSingleValueLength()) + "\"");
        }
        if (options.includeParameters() && parameters != null) {
            append(message, options, "parameters=" + maskParameters(parameters, options));
        }
        return limited(message, options.maxMessageLength());
    }

    static String batch(BatchExecutionObservation observation,
                        SqlExecutionLogOptions options,
                        SqlExecutionLogSelection selection,
                        SqlTransactionSource transactionSource) {
        if (!selection.shouldLog(observation)) {
            return null;
        }
        StringBuilder message = new StringBuilder(Math.min(options.maxMessageLength(), 256));
        append(message, options, "kind=BATCH");
        append(message, options, "backend=" + observation.backend());
        append(message, options, "eventType=" + observation.eventType());
        append(message, options, "statementType=" + observation.statementType());
        append(message, options, "status=" + batchStatus(observation));
        if (observation.failureCategory() != SqlFailureCategory.NONE) {
            append(message, options, "failureCategory=" + observation.failureCategory());
        }
        if (selection.includeDuration()) {
            append(message, options, "durationNanos=" + observation.durationNanos());
        }
        appendBatchDetails(message, options, selection, observation);
        append(message, options, "transactionSource=" + transactionSource);
        if (options.includeSql()) {
            append(message, options, "sql=\"" + maskSql(observation.sql(), options.maxSingleValueLength()) + "\"");
        }
        return limited(message, options.maxMessageLength());
    }

    /** 只输出固定清理分类，不读取驱动异常原文、SQL 或参数。 */
    static String resourceCleanup(ResourceCleanupObservation observation,
                                  SqlExecutionLogOptions options) {
        StringBuilder message = new StringBuilder(Math.min(options.maxMessageLength(), 192));
        append(message, options, "kind=RESOURCE_CLEANUP");
        append(message, options, "operation=" + observation.operation());
        append(message, options, "phase=" + observation.phase());
        append(message, options, "outcomeConfirmed=" + observation.outcomeConfirmed());
        append(message, options, "failureKind=" + observation.failureKind());
        return limited(message, options.maxMessageLength());
    }

    private static String batchStatus(BatchExecutionObservation observation) {
        return switch (observation) {
            case BatchExecutionObservation.Chunk chunk -> chunk.status().name();
            case BatchExecutionObservation.Summary summary -> summary.status().name();
            case BatchExecutionObservation.Recovery recovery -> recovery.status().name();
        };
    }

    private static void appendBatchDetails(StringBuilder message,
                                           SqlExecutionLogOptions options,
                                           SqlExecutionLogSelection selection,
                                           BatchExecutionObservation observation) {
        switch (observation) {
            case BatchExecutionObservation.Chunk chunk -> {
                appendBatchCounts(message, options, selection, chunk.inputCount(), chunk.affectedRows());
                append(message, options, "chunkIndex=" + chunk.chunkIndex());
                append(message, options, "parameterCount=" + chunk.parameterCount());
            }
            case BatchExecutionObservation.Summary summary -> {
                appendBatchCounts(message, options, selection, summary.inputCount(), summary.affectedRows());
                append(message, options, "chunkIndex=" + BatchExecutionObservation.NO_CHUNK);
                append(message, options, "chunkCount=" + summary.chunkCount());
                append(message, options, "successfulChunkCount=" + summary.successfulChunkCount());
                append(message, options, "failedChunkCount=" + summary.failedChunkCount());
                append(message, options, "parameterCount=" + summary.parameterCount());
            }
            case BatchExecutionObservation.Recovery recovery -> {
                appendBatchCounts(message, options, selection, 0, 0);
                append(message, options, "chunkIndex=" + recovery.chunkIndex());
                append(message, options, "parameterCount=0");
            }
        }
    }

    private static void appendBatchCounts(StringBuilder message,
                                          SqlExecutionLogOptions options,
                                          SqlExecutionLogSelection selection,
                                          long inputCount,
                                          long affectedRows) {
        append(message, options, "inputCount=" + inputCount);
        if (selection.includeAffectedRows()) {
            append(message, options, "affectedRows=" + affectedRows);
        }
    }

    private static String maskParameters(List<Object> parameters, SqlExecutionLogOptions options) {
        StringBuilder values = new StringBuilder("[");
        for (int index = 0; index < parameters.size(); index++) {
            if (index > 0) {
                values.append(", ");
            }
            values.append(maskValue(index, parameters.get(index), options));
            if (values.length() > options.maxMessageLength()) {
                values.setLength(Math.max(1, options.maxMessageLength() - 3));
                values.append("...");
                break;
            }
        }
        return values.append(']').toString();
    }

    private static String maskValue(int parameterIndex, Object value, SqlExecutionLogOptions options) {
        Object ruleValue = value instanceof SqlNullParameter ? null : value;
        if (fullyMasked(parameterIndex, ruleValue, options.redactionRule())) {
            return "<masked>";
        }
        // byte[] 和 char[] 无论扩展规则如何配置都绝不展示内容，这是不可放宽的底线。
        if (value instanceof byte[] bytes) {
            return "<masked byte[] length=" + bytes.length + ">";
        }
        if (value instanceof char[] chars) {
            return "<masked char[] length=" + chars.length + ">";
        }
        if (value instanceof SqlNullParameter) {
            return "null";
        }
        if (value == null) {
            return "null";
        }
        if (value instanceof CharSequence text) {
            return maskText(text, options.maxSingleValueLength());
        }
        return "<masked " + value.getClass().getSimpleName() + ">";
    }

    private static boolean fullyMasked(int parameterIndex,
                                       Object value,
                                       SqlParameterRedactionRule rule) {
        try {
            return rule.fullyMask(parameterIndex, value == null ? Object.class : value.getClass());
        } catch (RuntimeException failure) {
            // 脱敏扩展出错时宁可少记信息，也不能把参数按较宽松路径写出去。
            return true;
        }
    }

    private static String maskText(CharSequence text, int maxLength) {
        int length = text.length();
        if (length == 0) {
            return maxLength == 1 ? "*" : "\"\"";
        }
        if (maxLength <= 2) {
            return "*".repeat(maxLength);
        }
        int visibleStars = Math.min(Math.max(1, length - 2), maxLength - 2);
        StringBuilder masked = new StringBuilder(maxLength);
        masked.append(oneLineCharacter(text.charAt(0)));
        masked.append("*".repeat(visibleStars));
        if (length > 1 && masked.length() < maxLength) {
            masked.append(oneLineCharacter(text.charAt(length - 1)));
        }
        if (length > maxLength) {
            if (masked.length() + 3 > maxLength) {
                masked.setLength(Math.max(0, maxLength - 3));
            }
            masked.append("...");
        }
        String rendered = '"' + masked.toString() + '"';
        if (rendered.length() <= maxLength) {
            return rendered;
        }
        // 单值预算包含外层引号。预算很小时直接全部隐藏，不能为了日志格式多写两个字符。
        if (maxLength <= 3) {
            return "*".repeat(maxLength);
        }
        int contentLength = maxLength - 2;
        return '"' + "*".repeat(contentLength) + '"';
    }

    private static String maskSql(String sql, int maxLength) {
        StringBuilder masked = new StringBuilder(Math.min(sql.length(), maxLength));
        try {
            SqlLexicalScanner.scan(sql, SqlLexicalScanner.genericRules(), false,
                    (kind, start, end) -> appendMaskedSqlSegment(masked, sql, kind, start, end, maxLength));
        } catch (IllegalArgumentException malformedSql) {
            return limitedValue("<invalid SQL>", maxLength);
        }
        if (masked.length() > maxLength || sql.length() > maxLength) {
            if (maxLength <= 3) {
                return ".".repeat(maxLength);
            }
            masked.setLength(Math.min(masked.length(), maxLength - 3));
            masked.append("...");
        }
        return masked.toString();
    }

    private static void appendMaskedSqlSegment(StringBuilder target,
                                               String sql,
                                               SqlLexicalScanner.SegmentKind kind,
                                               int start,
                                               int end,
                                               int maxLength) {
        if (target.length() >= maxLength) {
            return;
        }
        if (kind == SqlLexicalScanner.SegmentKind.SINGLE_QUOTED
                || kind == SqlLexicalScanner.SegmentKind.DOUBLE_QUOTED) {
            appendRaw(target, sql, start, start + 1, maxLength);
            appendRaw(target, "***", 0, 3, maxLength);
            appendRaw(target, sql, end - 1, end, maxLength);
            return;
        }
        if (kind == SqlLexicalScanner.SegmentKind.DOLLAR_QUOTED) {
            int delimiterEnd = sql.indexOf('$', start + 1) + 1;
            int delimiterLength = delimiterEnd - start;
            appendRaw(target, sql, start, delimiterEnd, maxLength);
            appendRaw(target, "***", 0, 3, maxLength);
            appendRaw(target, sql, end - delimiterLength, end, maxLength);
            return;
        }
        if (kind == SqlLexicalScanner.SegmentKind.ORACLE_QUOTED) {
            int qOffset = sql.charAt(start) == 'n' || sql.charAt(start) == 'N' ? start + 1 : start;
            appendRaw(target, sql, start, qOffset + 3, maxLength);
            appendRaw(target, "***", 0, 3, maxLength);
            appendRaw(target, sql, end - 2, end, maxLength);
            return;
        }
        for (int index = start; index < end && target.length() < maxLength; index++) {
            target.append(oneLineCharacter(sql.charAt(index)));
        }
    }

    private static char oneLineCharacter(char value) {
        return Character.isISOControl(value) || value == '\u2028' || value == '\u2029' ? ' ' : value;
    }

    private static void appendRaw(StringBuilder target,
                                  String source,
                                  int start,
                                  int end,
                                  int maxLength) {
        int allowedEnd = Math.min(end, start + Math.max(0, maxLength - target.length()));
        if (start < allowedEnd) {
            target.append(source, start, allowedEnd);
        }
    }

    private static String limitedValue(String value, int maxLength) {
        if (value.length() <= maxLength) {
            return value;
        }
        return maxLength <= 3 ? ".".repeat(maxLength) : value.substring(0, maxLength - 3) + "...";
    }

    private static void append(StringBuilder message, SqlExecutionLogOptions options, String field) {
        if (message.length() > 0 && message.length() < options.maxMessageLength()) {
            message.append(' ');
        }
        int remaining = options.maxMessageLength() - message.length();
        if (remaining <= 0) {
            return;
        }
        if (field.length() <= remaining) {
            message.append(field);
            return;
        }
        if (remaining <= 3) {
            message.append(field, 0, remaining);
        } else {
            message.append(field, 0, remaining - 3).append("...");
        }
    }

    private static String limited(StringBuilder message, int maxLength) {
        if (message.length() <= maxLength) {
            return message.toString();
        }
        if (maxLength <= 3) {
            return ".".repeat(maxLength);
        }
        message.setLength(maxLength - 3);
        return message.append("...").toString();
    }
}
