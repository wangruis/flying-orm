package com.flying.orm.rdb.template;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.template.SqlStatements;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 把已注册模板渲染为参数化 SqlRequest。扫描器只识别引号外的占位符，PostgreSQL 的 {@code ::type}
 * 会原样保留；模板里出现的业务值永远不会通过字符串替换进入 SQL。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class SqlTemplateEngine {

    private final SqlTemplateRegistry registry;
    private final RdbDialect dialect;
    private final ValueCodecRegistry valueCodecs;
    /** JDBC 固定使用问号；R2DBC 则使用当前驱动原生标记，二者仍共用同一套安全扫描器。 */
    private final boolean jdbcBindMarkers;
    private SqlTemplateEngine(SqlTemplateRegistry registry, RdbDialect dialect, ValueCodecRegistry valueCodecs,
                              boolean jdbcBindMarkers) {
        this.registry = Objects.requireNonNull(registry, "SQL template registry must not be null");
        this.dialect = Objects.requireNonNull(dialect, "RDB dialect must not be null");
        this.valueCodecs = Objects.requireNonNull(valueCodecs, "value codec registry must not be null");
        this.jdbcBindMarkers = jdbcBindMarkers;
        for (SqlTemplate template : registry.templates()) {
            SqlTemplate.requireReadOnlyQuery(template.sql(), dialect);
        }
    }
    public static SqlTemplateEngine create(SqlTemplateRegistry registry, RdbDialect dialect,
                                           ValueCodecRegistry valueCodecs) {
        return new SqlTemplateEngine(registry, dialect, valueCodecs, false);
    }
    /**
     * 返回使用 JDBC 问号参数标记的模板引擎。只给同步 Operator 的跨包装配使用，不属于业务公共 API。
     */
    @InternalApi
    public SqlTemplateEngine forJdbc() {
        return jdbcBindMarkers ? this : new SqlTemplateEngine(registry, dialect, valueCodecs, true);
    }

    public SqlRequest render(String templateId, Map<String, ?> values, Map<String, String> identifiers) {
        SqlTemplate template = registry.template(templateId);
        return renderTemplate(template, values, identifiers, dialect, valueCodecs, jdbcBindMarkers);
    }

    /**
     * 把后端代码里直接写下来的单条 SQL 编译成可执行请求。
     * <p>这个入口只处理 {@code :name} 值参数，不开放动态表名或动态列名。SQL 结构既然已经由开发者直接写出，
     * 再允许调用方替换标识符只会扩大注入边界；确实需要受控标识符时继续使用注册模板。</p>
     * @param sql 后端代码或可信配置中的单条 SQL
     * @param values 命名参数；参数集合必须和 SQL 里的占位符完全一致
     * @param dialect 当前数据库方言，用来生成 R2DBC 驱动真正认识的参数标记
     * @param valueCodecs 应用统一使用的值转换规则
     * @return 使用当前 R2DBC 驱动标记并按出现顺序排列参数的原生请求
     */
    public static SqlRequest compileNative(String sql, Map<String, ?> values, RdbDialect dialect,
                                           ValueCodecRegistry valueCodecs) {
        SqlTemplate template = SqlTemplate.nativeStatement(sql);
        return renderTemplate(template, values, Map.of(),
                              Objects.requireNonNull(dialect, "RDB dialect must not be null"),
                              valueCodecs, false);
    }

    /** 同步 JDBC 使用同一安全扫描过程，但参数标记固定生成 {@code ?}。 */
    @InternalApi
    public static SqlRequest compileNativeJdbc(String sql, Map<String, ?> values, RdbDialect dialect,
                                               ValueCodecRegistry valueCodecs) {
        return renderTemplate(SqlTemplate.nativeStatement(sql), values, Map.of(), dialect, valueCodecs, true);
    }

    private static SqlRequest renderTemplate(SqlTemplate template, Map<String, ?> values,
                                             Map<String, String> identifiers, RdbDialect dialect,
                                             ValueCodecRegistry valueCodecs,
                                             boolean jdbcBindMarkers) {
        // Map.copyOf 会拒绝 null 值，而 SQL 参数允许显式绑定 null；这里只复制键和值，不改变参数语义。
        Map<String, ?> safeValues = new LinkedHashMap<>(Objects.requireNonNull(
                values, "SQL template values must not be null"));
        Map<String, String> safeIdentifiers = Map.copyOf(Objects.requireNonNull(
                identifiers, "SQL template identifiers must not be null"));
        if (!safeIdentifiers.keySet().equals(template.identifierSlots())) {
            throw new IllegalArgumentException("SQL template identifier slots do not match registered slots");
        }

        StringBuilder sql = new StringBuilder(template.sql().length());
        List<Object> parameters = new ArrayList<>();
        Set<String> usedValues = new LinkedHashSet<>();
        Set<String> usedIdentifiers = new LinkedHashSet<>();
        scan(template,
             safeValues,
             safeIdentifiers,
             sql,
             parameters,
             usedValues,
             usedIdentifiers,
             dialect,
             Objects.requireNonNull(valueCodecs, "value codec registry must not be null"),
             jdbcBindMarkers);

        if (!usedValues.equals(safeValues.keySet())) {
            throw new IllegalArgumentException("SQL template values do not match placeholders");
        }
        if (!usedIdentifiers.equals(template.identifierSlots())) {
            throw new IllegalArgumentException("registered identifier slots are not all used by SQL template");
        }
        return SqlRequest.nativeSql(sql.toString(), parameters);
    }

    private static void scan(SqlTemplate template, Map<String, ?> values, Map<String, String> identifiers,
                             StringBuilder sql, List<Object> parameters, Set<String> usedValues,
                             Set<String> usedIdentifiers, RdbDialect dialect,
                             ValueCodecRegistry valueCodecs,
                             boolean jdbcBindMarkers) {
        String source = template.sql();
        String dialectName = Objects.requireNonNull(dialect, "RDB dialect must not be null").name();
        boolean mysql = "mysql".equalsIgnoreCase(dialectName);
        boolean oracle = "oracle".equalsIgnoreCase(dialectName);
        boolean postgresql = "postgresql".equalsIgnoreCase(dialectName);
        boolean sqlServer = "sqlserver".equalsIgnoreCase(dialectName)
                || "sql-server".equalsIgnoreCase(dialectName);
        ScanState state = ScanState.PLAIN;
        int blockCommentDepth = 0;
        String dollarQuoteDelimiter = null;
        for (int index = 0; index < source.length();) {
            char current = source.charAt(index);
            char next = index + 1 < source.length() ? source.charAt(index + 1) : '\0';
            if (state == ScanState.DOLLAR_QUOTE) {
                if (source.startsWith(dollarQuoteDelimiter, index)) {
                    sql.append(dollarQuoteDelimiter);
                    index += dollarQuoteDelimiter.length();
                    dollarQuoteDelimiter = null;
                    state = ScanState.PLAIN;
                } else {
                    sql.append(current);
                    index++;
                }
                continue;
            }
            if (state == ScanState.BLOCK_COMMENT) {
                if ((postgresql || sqlServer) && current == '/' && next == '*') {
                    blockCommentDepth++;
                    sql.append(current).append(next);
                    index += 2;
                    continue;
                }
                if (current == '*' && next == '/') {
                    blockCommentDepth--;
                    sql.append(current).append(next);
                    index += 2;
                    if (blockCommentDepth == 0) {
                        state = ScanState.PLAIN;
                    }
                    continue;
                }
                sql.append(current);
                index++;
                continue;
            }
            if (state == ScanState.LINE_COMMENT) {
                sql.append(current);
                index++;
                if (current == '\n' || current == '\r') {
                    state = ScanState.PLAIN;
                }
                continue;
            }
            if (state == ScanState.SINGLE_QUOTE || state == ScanState.DOUBLE_QUOTE
                    || state == ScanState.BACKTICK_IDENTIFIER || state == ScanState.BRACKET_IDENTIFIER) {
                char closing = state.closingCharacter();
                sql.append(current);
                if (current == closing && next == closing) {
                    sql.append(next);
                    index += 2;
                    continue;
                }
                index++;
                if (current == closing) {
                    state = ScanState.PLAIN;
                }
                continue;
            }
            if (SqlTemplate.isLineCommentStart(source, index, mysql)) {
                state = ScanState.LINE_COMMENT;
                sql.append(current).append(next);
                index += 2;
                continue;
            }
            if (mysql && current == '#') {
                state = ScanState.LINE_COMMENT;
                sql.append(current);
                index++;
                continue;
            }
            if (current == '/' && next == '*') {
                state = ScanState.BLOCK_COMMENT;
                blockCommentDepth = 1;
                sql.append(current).append(next);
                index += 2;
                continue;
            }
            if (oracle) {
                int oracleQuoteEnd = SqlStatements.oracleAlternativeQuoteEnd(source, index);
                if (oracleQuoteEnd >= 0) {
                    sql.append(source, index, oracleQuoteEnd);
                    index = oracleQuoteEnd;
                    continue;
                }
            }
            if (current == '\'') {
                state = ScanState.SINGLE_QUOTE;
                sql.append(current);
                index++;
                continue;
            }
            if (current == '"') {
                state = ScanState.DOUBLE_QUOTE;
                sql.append(current);
                index++;
                continue;
            }
            if (current == '`') {
                state = ScanState.BACKTICK_IDENTIFIER;
                sql.append(current);
                index++;
                continue;
            }
            if (sqlServer && current == '[') {
                state = ScanState.BRACKET_IDENTIFIER;
                sql.append(current);
                index++;
                continue;
            }
            if (postgresql && current == '$') {
                String delimiter = SqlTemplate.dollarQuoteDelimiterAt(source, index);
                if (delimiter != null) {
                    dollarQuoteDelimiter = delimiter;
                    state = ScanState.DOLLAR_QUOTE;
                    sql.append(delimiter);
                    index += delimiter.length();
                    continue;
                }
            }
            if (current == '$' && index + 1 < source.length() && source.charAt(index + 1) == '{') {
                int end = source.indexOf('}', index + 2);
                if (end < 0) {
                    throw new IllegalArgumentException("SQL template identifier slot is not closed");
                }
                String name = SqlTemplate.requireName(source.substring(index + 2, end), "identifier slot");
                if (!template.identifierSlots().contains(name)) {
                    throw new IllegalArgumentException("SQL template identifier slot is not registered");
                }
                sql.append(Objects.requireNonNull(dialect,
                                                  "RDB dialect is required for identifier slots")
                                  .schema()
                                  .identifier(identifiers.get(name)));
                usedIdentifiers.add(name);
                index = end + 1;
                continue;
            }
            if (current == ':' && (index == 0 || source.charAt(index - 1) != ':')
                    && index + 1 < source.length() && source.charAt(index + 1) != ':'
                    && Character.isJavaIdentifierStart(source.charAt(index + 1))) {
                int end = index + 2;
                while (end < source.length() && Character.isJavaIdentifierPart(source.charAt(end))) {
                    end++;
                }
                String name = source.substring(index + 1, end);
                if (!values.containsKey(name)) {
                    throw new IllegalArgumentException("SQL template value is missing");
                }
                appendBindMarker(sql, dialect, parameters.size(), jdbcBindMarkers);
                parameters.add(valueCodecs.write(values.get(name)));
                usedValues.add(name);
                index = end;
                continue;
            }
            sql.append(current);
            index++;
        }
        if (state != ScanState.PLAIN && state != ScanState.LINE_COMMENT) {
            throw new IllegalArgumentException("SQL template contains an unclosed quoted value or block comment");
        }
    }

    /** 模板扫描只关心会包住占位符的 SQL 文本区域，不在这里尝试解析完整 SQL 语法。 */
    private enum ScanState {
        PLAIN('\0'),
        SINGLE_QUOTE('\''),
        DOUBLE_QUOTE('"'),
        BACKTICK_IDENTIFIER('`'),
        BRACKET_IDENTIFIER(']'),
        DOLLAR_QUOTE('\0'),
        LINE_COMMENT('\0'),
        BLOCK_COMMENT('\0');

        private final char closingCharacter;

        ScanState(char closingCharacter) {
            this.closingCharacter = closingCharacter;
        }

        private char closingCharacter() {
            return closingCharacter;
        }
    }

    /**
     * 模板请求会标成 NATIVE，执行器不会再改写参数标记，所以这里必须一次生成当前驱动认识的格式。
     * 只在识别到命名参数时追加标记，原 SQL 自带的问号（例如 PostgreSQL JSON 运算符）会原样保留。
     */
    private static void appendBindMarker(StringBuilder sql, RdbDialect dialect, int parameterIndex,
                                         boolean jdbcBindMarkers) {
        if (jdbcBindMarkers) {
            sql.append('?');
            return;
        }
        String dialectName = Objects.requireNonNull(dialect, "RDB dialect must not be null").name();
        if ("postgresql".equals(dialectName)) {
            sql.append('$').append(parameterIndex + 1);
            return;
        }
        if ("sqlserver".equals(dialectName) || "sql-server".equals(dialectName)) {
            sql.append("@P").append(parameterIndex);
            return;
        }
        sql.append('?');
    }
}
