package com.flying.orm.rdb.observation;

/**
 * SQL 执行日志的安全开关和长度预算。
 *
 * <p>默认只记录结构化执行结果，不把 SQL 文本和参数值写进日志。参数值即使显式打开，
 * 也会先经过内置脱敏，再受单值和整条日志长度限制。这样日志配置失误时，最多影响排查信息，
 * 不会把一整批业务数据复制到内存里。</p>
 * @author wangr
 * @version v1.0
 */
public record SqlExecutionLogOptions(boolean includeSql,
                                     boolean includeParameters,
                                     int maxSingleValueLength,
                                     int maxMessageLength,
                                     SqlParameterRedactionRule redactionRule) {

    private static final int DEFAULT_SINGLE_VALUE_LENGTH = 128;

    private static final int DEFAULT_MESSAGE_LENGTH = 4096;

    /** 默认只记 SQL 类型、状态、耗时和结果数量。 */
    public static SqlExecutionLogOptions defaults() {
        return new SqlExecutionLogOptions(false,
                                           false,
                                           DEFAULT_SINGLE_VALUE_LENGTH,
                                           DEFAULT_MESSAGE_LENGTH,
                                           SqlParameterRedactionRule.none());
    }

    /**
     * 保留原来的四参数构造方式。没有额外规则时，直接使用这个构造器即可。
     */
    public SqlExecutionLogOptions(boolean includeSql,
                                  boolean includeParameters,
                                  int maxSingleValueLength,
                                  int maxMessageLength) {
        this(includeSql,
             includeParameters,
             maxSingleValueLength,
             maxMessageLength,
             SqlParameterRedactionRule.none());
    }

    public SqlExecutionLogOptions {
        if (maxSingleValueLength <= 0) {
            throw new IllegalArgumentException("max single value length must be positive");
        }
        if (maxMessageLength <= 0) {
            throw new IllegalArgumentException("max message length must be positive");
        }
        redactionRule = java.util.Objects.requireNonNull(redactionRule,
                                                        "sql parameter redaction rule must not be null");
    }

    public SqlExecutionLogOptions withSql(boolean enabled) {
        return new SqlExecutionLogOptions(enabled,
                                           includeParameters,
                                           maxSingleValueLength,
                                           maxMessageLength,
                                           redactionRule);
    }

    public SqlExecutionLogOptions withParameters(boolean enabled) {
        return new SqlExecutionLogOptions(includeSql,
                                           enabled,
                                           maxSingleValueLength,
                                           maxMessageLength,
                                           redactionRule);
    }

    public SqlExecutionLogOptions withLimits(int singleValueLength, int messageLength) {
        return new SqlExecutionLogOptions(includeSql,
                                           includeParameters,
                                           singleValueLength,
                                           messageLength,
                                           redactionRule);
    }

    /**
     * 追加一条更严格的参数隐藏规则。规则不能关闭内置脱敏，也不能改变日志长度上限。
     */
    public SqlExecutionLogOptions withRedactionRule(SqlParameterRedactionRule rule) {
        return new SqlExecutionLogOptions(includeSql,
                                           includeParameters,
                                           maxSingleValueLength,
                                           maxMessageLength,
                                           rule);
    }
}
