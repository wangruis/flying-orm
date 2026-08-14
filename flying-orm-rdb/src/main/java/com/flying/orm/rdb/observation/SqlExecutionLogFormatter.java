package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.internal.template.SqlStatements;

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
        append(message, options, "inputCount=" + observation.inputCount());
        if (selection.includeAffectedRows()) {
            append(message, options, "affectedRows=" + observation.affectedRows());
        }
        append(message, options, "chunkIndex=" + observation.chunkIndex());
        if (observation.eventType() == BatchExecutionEventType.SUMMARY) {
            append(message, options, "chunkCount=" + observation.chunkCount());
            append(message, options, "successfulChunkCount=" + observation.successfulChunkCount());
            append(message, options, "failedChunkCount=" + observation.failedChunkCount());
        }
        append(message, options, "parameterCount=" + observation.parameterCount());
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
        return switch (observation.eventType()) {
            case CHUNK -> observation.chunkStatus().name();
            case SUMMARY -> observation.summaryStatus().name();
            case RECOVERY -> observation.recoveryStatus().name();
        };
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
        // byte[] 和 char[] 无论扩展规则如何配置都绝不展示内容，这是不可放宽的底线。
        if (value instanceof byte[] bytes) {
            return "<masked byte[] length=" + bytes.length + ">";
        }
        if (value instanceof char[] chars) {
            return "<masked char[] length=" + chars.length + ">";
        }
        if (fullyMasked(parameterIndex, value, options.redactionRule())) {
            return "<masked>";
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
            ObservationFailureSupport.rethrowVirtualMachineError(failure);
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
        masked.append(text.charAt(0));
        masked.append("*".repeat(visibleStars));
        if (length > 1 && masked.length() < maxLength) {
            masked.append(text.charAt(length - 1));
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
        for (int index = 0; index < sql.length() && masked.length() < maxLength;) {
            char current = sql.charAt(index);
            try {
                int oracleQuoteEnd = SqlStatements.oracleAlternativeQuoteEnd(sql, index);
                if (oracleQuoteEnd >= 0) {
                    int quotePrefixOffset = current == 'n' || current == 'N' ? index + 1 : index;
                    int openingDelimiterOffset = quotePrefixOffset + 2;
                    masked.append(sql, index, openingDelimiterOffset + 1)
                          .append("***")
                          .append(sql.charAt(oracleQuoteEnd - 2))
                          .append('\'');
                    index = oracleQuoteEnd;
                    continue;
                }
            } catch (IllegalArgumentException unclosedAlternativeQuote) {
                // 日志脱敏遇到损坏 SQL 时必须隐藏剩余文本，不能让观测路径反向覆盖执行结果。
                masked.append(current).append("'***");
                index = sql.length();
                continue;
            }
            if (current == '\'') {
                masked.append("'***");
                index++;
                while (index < sql.length()) {
                    if (sql.charAt(index) == '\\' && index + 1 < sql.length()) {
                        index += 2;
                        continue;
                    }
                    if (sql.charAt(index) == '\'') {
                        // SQL 标准用两个连续单引号表示文本里的一个单引号，不能把第二个误当成新文本。
                        if (index + 1 < sql.length() && sql.charAt(index + 1) == '\'') {
                            index += 2;
                            continue;
                        }
                        masked.append('\'');
                        index++;
                        break;
                    }
                    index++;
                }
                continue;
            }
            if (current == '$') {
                String delimiter = dollarQuoteDelimiterAt(sql, index);
                if (delimiter != null) {
                    masked.append(delimiter).append("***");
                    int contentStart = index + delimiter.length();
                    int closing = sql.indexOf(delimiter, contentStart);
                    if (closing >= 0) {
                        masked.append(delimiter);
                        index = closing + delimiter.length();
                    } else {
                        // 未闭合文本也不能把剩余内容写进日志。
                        index = sql.length();
                    }
                    continue;
                }
            }
            masked.append(current);
            index++;
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

    /** 识别 PostgreSQL 的 {@code $$} 和 {@code $tag$} 文本边界。 */
    private static String dollarQuoteDelimiterAt(String sql, int offset) {
        int end = sql.indexOf('$', offset + 1);
        if (end < 0) {
            return null;
        }
        if (end == offset + 1) {
            return "$$";
        }
        char first = sql.charAt(offset + 1);
        if (!(Character.isLetter(first) || first == '_')) {
            return null;
        }
        for (int index = offset + 2; index < end; index++) {
            char character = sql.charAt(index);
            if (!(Character.isLetterOrDigit(character) || character == '_')) {
                return null;
            }
        }
        return sql.substring(offset, end + 1);
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
