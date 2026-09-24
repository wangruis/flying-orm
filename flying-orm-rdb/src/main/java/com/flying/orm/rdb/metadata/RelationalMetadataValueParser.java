package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.core.type.LogicalType;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * 把数据库字典中的默认值和 CHECK 文本还原成 flying-orm 的封闭关系模型。
 *
 * <p>这里只接受 {@code RelationalSchemaSqlRenderer} 能生成的语法。数据库中出现函数、任意表达式、
 * 自定义操作符或其他无法无损表达的结构时会明确失败，不能把原始 SQL 带回模型，也不能猜成另一个
 * 谓词。该解析只发生在显式 Schema 读取冷路径。</p>
 */
final class RelationalMetadataValueParser {

    private static final Pattern POSTGRESQL_LITERAL_CAST_SUFFIX = Pattern.compile(
            "(?i)(?:\\s*::\\s*(?:[a-z_][a-z0-9_$]*\\.)?[a-z_][a-z0-9_$]*"
                    + "(?:\\s+(?:varying|precision|with\\s+time\\s+zone|without\\s+time\\s+zone))?"
                    + "(?:\\s*\\[\\s*\\])?)+\\s*");
    private static final Pattern MYSQL_TEMPORAL_KEYWORD_PRECISION = Pattern.compile(
            "(?i)^(CURRENT_TIME|CURTIME|CURRENT_TIMESTAMP)\\s*\\(\\s*\\d+\\s*\\)$");
    private static final Pattern ORACLE_TEMPORAL_LITERAL = Pattern.compile("(?i)(DATE|TIMESTAMP)\\s*'([^']*)'");
    private static final Pattern SQL_SERVER_TEMPORAL_CAST = Pattern.compile(
            "(?i)^CAST\\(\\s*(?:CURRENT_TIMESTAMP|GETDATE\\(\\))\\s+AS\\s+(DATE|TIME)\\s*\\)$");
    private static final Pattern SQL_SERVER_TEMPORAL_CONVERT = Pattern.compile(
            "(?i)^CONVERT\\(\\s*\\[?(DATE|TIME)\\]?\\s*,\\s*"
                    + "(?:CURRENT_TIMESTAMP|GETDATE\\(\\))\\s*(?:,\\s*0\\s*)?\\)$");
    private static final Pattern H2_NUMERIC_LITERAL_CAST = Pattern.compile(
            "(?i)(?<![a-z0-9_])CAST\\(\\s*"
                    + "([+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][+-]?\\d+)?)"
                    + "\\s+AS\\s+(?:NUMERIC|DECIMAL)"
                    + "(?:\\s*\\(\\s*\\d+\\s*(?:,\\s*\\d+\\s*)?\\))?\\s*\\)"
                    + "(?![a-z0-9_])");

    private RelationalMetadataValueParser() {
    }

    static ColumnDefault columnDefault(
            String expression,
            DatabaseType databaseType,
            InformationSchemaFormMetadataReader.SnapshotDialect dialect) {
        if (expression == null) {
            return ColumnDefault.none();
        }
        if (dialect == InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL
                && databaseType.logicalType() == LogicalType.TEXT) {
            // MySQL 的 COLUMN_DEFAULT 是字典值，不是带引号的 SQL 字面量。
            return ColumnDefault.literal(expression);
        }
        String value = stripOuterParentheses(expression.trim());
        if ((dialect == InformationSchemaFormMetadataReader.SnapshotDialect.ORACLE
                || dialect == InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER)
                && "NULL".equalsIgnoreCase(value)) {
            return ColumnDefault.none();
        }
        if (dialect == InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER) {
            var cast = SQL_SERVER_TEMPORAL_CAST.matcher(value);
            var convert = SQL_SERVER_TEMPORAL_CONVERT.matcher(value);
            if (cast.matches() || convert.matches()) {
                String type = cast.matches() ? cast.group(1) : convert.group(1);
                return "DATE".equalsIgnoreCase(type)
                        ? ColumnDefault.currentDate() : ColumnDefault.currentTime();
            }
            if (value.length() > 1 && (value.charAt(0) == 'N' || value.charAt(0) == 'n')
                    && value.charAt(1) == '\'') {
                value = value.substring(1);
            }
        }
        String keywordValue = value;
        if (dialect == InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL) {
            keywordValue = MYSQL_TEMPORAL_KEYWORD_PRECISION.matcher(value).replaceFirst("$1");
        }
        if (dialect == InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER
                && "GETDATE()".equalsIgnoreCase(value)) {
            return ColumnDefault.currentTimestamp();
        }
        String keyword = keywordValue.replace("()", "").toUpperCase(Locale.ROOT);
        if (dialect == InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL) {
            keyword = switch (keyword) {
                case "CURDATE" -> "CURRENT_DATE";
                case "CURTIME" -> "CURRENT_TIME";
                default -> keyword;
            };
        }
        if ("CURRENT_DATE".equals(keyword)
                && acceptsTemporalKeyword(keyword, databaseType.logicalType(), dialect)) {
            return ColumnDefault.currentDate();
        }
        if ("CURRENT_TIME".equals(keyword)
                && acceptsTemporalKeyword(keyword, databaseType.logicalType(), dialect)) {
            return ColumnDefault.currentTime();
        }
        if ("CURRENT_TIMESTAMP".equals(keyword)
                && acceptsTemporalKeyword(keyword, databaseType.logicalType(), dialect)) {
            return ColumnDefault.currentTimestamp();
        }

        String literal = stripPostgresqlLiteralCast(value, databaseType, dialect);
        if (dialect == InformationSchemaFormMetadataReader.SnapshotDialect.ORACLE) {
            var temporal = ORACLE_TEMPORAL_LITERAL.matcher(literal);
            if (temporal.matches()) {
                return ColumnDefault.literal(oracleTemporalLiteral(
                        temporal.group(1), temporal.group(2), databaseType));
            }
        }
        if (isQuotedString(literal)) {
            return ColumnDefault.literal(convertString(unquoteString(literal), databaseType.logicalType()));
        }
        if (databaseType.logicalType() == LogicalType.BOOLEAN) {
            if ("true".equalsIgnoreCase(literal) || "1".equals(literal)) {
                return ColumnDefault.literal(true);
            }
            if ("false".equalsIgnoreCase(literal) || "0".equals(literal)) {
                return ColumnDefault.literal(false);
            }
        }
        if (databaseType.logicalType().numeric()) {
            return ColumnDefault.literal(number(literal, databaseType));
        }
        if (dialect == InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL) {
            return ColumnDefault.literal(convertString(literal, databaseType.logicalType()));
        }
        throw new IllegalStateException("column default cannot be represented safely");
    }

    static CheckPredicate checkPredicate(
            String expression,
            Map<String, DatabaseType> columnTypes,
            InformationSchemaFormMetadataReader.SnapshotDialect dialect) {
        String source = Objects.requireNonNull(expression, "check expression must not be null").trim();
        if (source.regionMatches(true, 0, "CHECK", 0, 5)) {
            source = source.substring(5).trim();
        }
        Parser parser = new Parser(source, columnTypes, dialect);
        CheckPredicate predicate = parser.parse();
        parser.requireEnd();
        return predicate;
    }

    private static Object convertString(String value, LogicalType type) {
        try {
            return switch (type) {
                case DATE -> LocalDate.parse(value);
                case TIME -> LocalTime.parse(value);
                case OFFSET_TIME -> OffsetTime.parse(value);
                case TIMESTAMP -> LocalDateTime.parse(value.replace(' ', 'T'));
                case OFFSET_TIMESTAMP -> OffsetDateTime.parse(value.replace(' ', 'T'));
                case UUID -> UUID.fromString(value);
                default -> value;
            };
        } catch (RuntimeException error) {
            throw new IllegalStateException("typed schema literal cannot be parsed safely", error);
        }
    }

    private static Object oracleTemporalLiteral(String keyword, String value, DatabaseType type) {
        if ("DATE".equalsIgnoreCase(keyword)
                && (type.logicalType() == LogicalType.DATE || "ORACLE_DATE".equals(type.baseName()))) {
            // 物理 Oracle DATE 被映射为 ORACLE_DATE；ANSI DATE 自身仍是无时间部分的 LocalDate。
            return convertString(value, LogicalType.DATE);
        }
        if ("TIMESTAMP".equalsIgnoreCase(keyword)) {
            if (type.logicalType() == LogicalType.TIMESTAMP) {
                return convertString(value, LogicalType.TIMESTAMP);
            }
            if (type.logicalType() == LogicalType.OFFSET_TIMESTAMP) {
                int offsetSeparator = value.lastIndexOf(' ');
                if (offsetSeparator > 10) {
                    return convertString(value.substring(0, offsetSeparator).replace(' ', 'T')
                            + value.substring(offsetSeparator + 1), LogicalType.OFFSET_TIMESTAMP);
                }
            }
        }
        throw new IllegalStateException("typed Oracle schema literal cannot be represented safely");
    }

    private static Object number(String value, DatabaseType type) {
        try {
            return switch (type.logicalType()) {
                case SMALL_INTEGER, INTEGER, BIG_INTEGER -> integerNumber(new BigDecimal(value), type);
                case DECIMAL -> new BigDecimal(value);
                case FLOAT -> Double.valueOf(value);
                default -> throw new IllegalStateException("column is not numeric");
            };
        } catch (NumberFormatException error) {
            throw new IllegalStateException("numeric schema literal cannot be parsed safely", error);
        }
    }

    /** Keep existing integer carriers when exact; CHECK operands need not fit the column width. */
    private static Object integerNumber(BigDecimal value, DatabaseType type) {
        try {
            return switch (type.logicalType()) {
                case SMALL_INTEGER -> {
                    if (type.unsigned() || "MEDIUMINT".equals(type.baseName())) {
                        yield value.intValueExact();
                    }
                    yield value.shortValueExact();
                }
                case INTEGER -> {
                    if (type.unsigned()) {
                        yield value.longValueExact();
                    }
                    yield value.intValueExact();
                }
                case BIG_INTEGER -> {
                    if (type.unsigned()) {
                        yield value.toBigIntegerExact();
                    }
                    yield value.longValueExact();
                }
                default -> throw new IllegalStateException("column is not integral");
            };
        } catch (ArithmeticException outsideIntegerCarrier) {
            return value;
        }
    }

    private static boolean acceptsTemporalKeyword(
            String keyword,
            LogicalType type,
            InformationSchemaFormMetadataReader.SnapshotDialect dialect) {
        if (dialect != InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL) {
            return true;
        }
        return switch (keyword) {
            case "CURRENT_DATE" -> type == LogicalType.DATE;
            case "CURRENT_TIME" -> type == LogicalType.TIME;
            case "CURRENT_TIMESTAMP" -> type == LogicalType.TIMESTAMP
                    || type == LogicalType.OFFSET_TIMESTAMP;
            default -> false;
        };
    }

    private static Object checkNumber(
            String value,
            DatabaseType type,
            InformationSchemaFormMetadataReader.SnapshotDialect dialect) {
        if (type.logicalType() != LogicalType.BOOLEAN) {
            return number(value, type);
        }
        if (dialect != InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER
                && dialect != InformationSchemaFormMetadataReader.SnapshotDialect.ORACLE) {
            throw new IllegalStateException("numeric boolean schema literal is not supported by dialect");
        }
        if ("0".equals(value)) {
            return false;
        }
        if ("1".equals(value)) {
            return true;
        }
        throw new IllegalStateException("boolean schema literal must be zero or one");
    }

    private static String stripPostgresqlLiteralCast(
            String value,
            DatabaseType type,
            InformationSchemaFormMetadataReader.SnapshotDialect dialect) {
        if (dialect != InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL) {
            return value;
        }
        if (isQuotedStringPrefix(value)) {
            int end = quotedStringEnd(value, 0);
            String suffix = value.substring(end + 1).trim();
            if (suffix.isEmpty() || isPostgresqlLiteralCastSuffix(suffix)) {
                validateDefaultCasts(value.substring(0, end + 1), suffix, type);
                return value.substring(0, end + 1);
            }
            return value;
        }
        int cast = value.indexOf("::");
        if (cast < 0 || !isPostgresqlLiteralCastSuffix(value.substring(cast))) {
            return value;
        }
        String literal = stripOuterParentheses(value.substring(0, cast).trim());
        validateDefaultCasts(literal, value.substring(cast), type);
        return literal;
    }

    private static void validateDefaultCasts(String literal, String suffix, DatabaseType type) {
        if (suffix.isEmpty()) return;
        Object value = isQuotedString(literal) ? convertString(unquoteString(literal), type.logicalType())
                : type.logicalType().numeric() ? number(literal, type)
                : "true".equalsIgnoreCase(literal) ? Boolean.TRUE
                : "false".equalsIgnoreCase(literal) ? Boolean.FALSE : literal;
        requireLosslessCasts(value, type, java.util.Arrays.stream(suffix.split("::"))
                .filter(cast -> !cast.isBlank()).toList());
    }

    /** 目录中的强转只有在字面量及比较语义不变时才能省略；不执行任意数据库表达式。 */
    private static void requireLosslessCasts(Object value, DatabaseType type, List<String> declarations) {
        List<String> casts = declarations.stream().map(declaration -> {
            String cast = declaration.trim().toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");
            return cast.startsWith("pg_catalog.") ? cast.substring("pg_catalog.".length()) : cast;
        }).toList();
        int floatingWidth = 0;
        for (String cast : casts) {
            int width = switch (cast) {
                case "real", "float4" -> 32;
                case "double precision", "float8" -> 64;
                default -> 0;
            };
            if (floatingWidth != 0 && width != floatingWidth) throw unsupported();
            floatingWidth = width;
        }
        for (int index = 0; index < casts.size(); index++) {
            String cast = casts.get(index);
            if (isLosslessCast(value, type, cast)) continue;
            // 自身浮点类型的字面量允许二进制舍入，但后续转回 numeric 等类型不能被静默抹去。
            if (sameFloatingType(type, cast)
                    && casts.subList(index, casts.size()).stream().allMatch(next -> sameFloatingType(type, next))) {
                try {
                    double number = Double.parseDouble(value.toString());
                    if (Double.isFinite(number) && (!cast.equals("real") && !cast.equals("float4")
                            || Float.isFinite((float) number))) return;
                } catch (NumberFormatException invalid) {
                    throw unsupported();
                }
            }
            throw unsupported();
        }
    }

    private static boolean sameFloatingType(DatabaseType type, String cast) {
        return !type.isArray() && switch (cast) {
            case "real", "float4" -> type.baseName().equals("REAL") || type.baseName().equals("FLOAT4");
            case "double precision", "float8" -> type.baseName().equals("DOUBLE PRECISION")
                    || type.baseName().equals("DOUBLE") || type.baseName().equals("FLOAT8");
            default -> false;
        };
    }

    private static boolean isLosslessCast(Object value, DatabaseType type, String cast) {
        boolean safe;
        try {
            if (type.logicalType().numeric() && !type.isArray()) {
                BigDecimal number = new BigDecimal(value.toString());
                safe = switch (cast) {
                    case "numeric", "decimal", "dec" -> true;
                    case "smallint", "int2" -> { number.shortValueExact(); yield true; }
                    case "integer", "int", "int4" -> { number.intValueExact(); yield true; }
                    case "bigint", "int8" -> { number.longValueExact(); yield true; }
                    case "real", "float4" -> new BigDecimal((double) number.floatValue()).compareTo(number) == 0;
                    case "double precision", "float8" -> new BigDecimal(number.doubleValue()).compareTo(number) == 0;
                    default -> false;
                };
            } else {
                safe = !type.isArray() && switch (cast) {
                    case "text", "varchar", "character varying" -> Parser.binaryTextType(type) && value instanceof String;
                    case "bpchar" -> value instanceof String && switch (type.baseName()) {
                        case "CHAR", "CHARACTER", "BPCHAR", "PG_CATALOG.BPCHAR" -> true;
                        default -> false;
                    };
                    case "boolean", "bool" -> type.logicalType() == LogicalType.BOOLEAN
                            && (value instanceof Boolean || "true".equals(value) || "false".equals(value));
                    case "date" -> type.logicalType() == LogicalType.DATE && value instanceof LocalDate;
                    case "time", "time without time zone" -> type.logicalType() == LogicalType.TIME && value instanceof LocalTime;
                    case "timetz", "time with time zone" -> type.logicalType() == LogicalType.OFFSET_TIME && value instanceof OffsetTime;
                    case "timestamp", "timestamp without time zone" -> type.logicalType() == LogicalType.TIMESTAMP && value instanceof LocalDateTime;
                    case "timestamptz", "timestamp with time zone" -> type.logicalType() == LogicalType.OFFSET_TIMESTAMP && value instanceof OffsetDateTime;
                    case "uuid" -> type.logicalType() == LogicalType.UUID && value instanceof UUID;
                    case "interval" -> type.logicalType() == LogicalType.INTERVAL;
                    case "json", "jsonb", "bytea", "xml" -> type.baseName().equalsIgnoreCase(cast);
                    default -> false;
                };
            }
        } catch (ArithmeticException | NumberFormatException invalid) {
            return false;
        }
        return safe;
    }

    private static boolean isPostgresqlLiteralCastSuffix(String value) {
        return POSTGRESQL_LITERAL_CAST_SUFFIX.matcher(value).matches();
    }

    private static String stripOuterParentheses(String source) {
        String value = source;
        while (value.length() > 1 && value.charAt(0) == '(' && matchingOuterParenthesis(value)) {
            value = value.substring(1, value.length() - 1).trim();
        }
        return value;
    }

    private static boolean matchingOuterParenthesis(String value) {
        int depth = 0;
        boolean quoted = false;
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (character == '\'') {
                if (quoted && index + 1 < value.length() && value.charAt(index + 1) == '\'') {
                    index++;
                } else {
                    quoted = !quoted;
                }
            } else if (!quoted && character == '(') {
                depth++;
            } else if (!quoted && character == ')' && --depth == 0) {
                return index == value.length() - 1;
            }
        }
        return false;
    }

    private static boolean isQuotedString(String value) {
        return isQuotedStringPrefix(value) && quotedStringEnd(value, 0) == value.length() - 1;
    }

    private static boolean isQuotedStringPrefix(String value) {
        return !value.isEmpty() && value.charAt(0) == '\'';
    }

    private static int quotedStringEnd(String value, int start) {
        for (int index = start + 1; index < value.length(); index++) {
            if (value.charAt(index) != '\'') {
                continue;
            }
            if (index + 1 < value.length() && value.charAt(index + 1) == '\'') {
                index++;
            } else {
                return index;
            }
        }
        throw new IllegalStateException("unterminated schema string literal");
    }

    private static String unquoteString(String value) {
        return value.substring(1, value.length() - 1).replace("''", "'");
    }

    private enum TokenType {
        IDENTIFIER,
        STRING,
        DATE_LITERAL,
        TIMESTAMP_LITERAL,
        NUMBER,
        TRUE,
        FALSE,
        NULL,
        AND,
        OR,
        NOT,
        IN,
        IS,
        ANY,
        ARRAY,
        LEFT_PAREN,
        RIGHT_PAREN,
        LEFT_BRACKET,
        RIGHT_BRACKET,
        COMMA,
        EQUAL,
        NOT_EQUAL,
        LESS,
        LESS_EQUAL,
        GREATER,
        GREATER_EQUAL,
        CAST,
        END
    }

    private record Token(TokenType type, String text) {
    }

    private static final class Parser {

        private final Lexer lexer;
        private final Map<String, DatabaseType> columnTypes;
        private final InformationSchemaFormMetadataReader.SnapshotDialect dialect;
        private Token current;

        private Parser(String source,
                       Map<String, DatabaseType> columnTypes,
                       InformationSchemaFormMetadataReader.SnapshotDialect dialect) {
            this.lexer = new Lexer(source, dialect);
            this.columnTypes = Map.copyOf(Objects.requireNonNull(
                    columnTypes, "check column types must not be null"));
            this.dialect = dialect;
            this.current = lexer.next();
        }

        private CheckPredicate parse() {
            return parseOr();
        }

        private CheckPredicate parseOr() {
            List<CheckPredicate> children = new ArrayList<>();
            children.add(parseAnd());
            while (match(TokenType.OR)) {
                children.add(parseAnd());
            }
            return logical(CheckPredicate.LogicalOperator.OR, children);
        }

        private CheckPredicate parseAnd() {
            List<CheckPredicate> children = new ArrayList<>();
            children.add(parseUnary());
            while (match(TokenType.AND)) {
                children.add(parseUnary());
            }
            return logical(CheckPredicate.LogicalOperator.AND, children);
        }

        private CheckPredicate parseUnary() {
            if (match(TokenType.NOT)) {
                expect(TokenType.LEFT_PAREN);
                CheckPredicate predicate = parseOr();
                expect(TokenType.RIGHT_PAREN);
                return CheckPredicate.not(predicate);
            }
            if (current.type() == TokenType.LEFT_PAREN && !parenthesizedColumn()) {
                advance();
                CheckPredicate predicate = parseOr();
                expect(TokenType.RIGHT_PAREN);
                return predicate;
            }
            return parseColumnPredicate();
        }

        /** Distinguish a parenthesized column operand from a parenthesized predicate. */
        private boolean parenthesizedColumn() {
            if (dialect != InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL) {
                return false;
            }
            int savedIndex = lexer.index;
            try {
                Token token = current;
                int depth = 0;
                while (token.type() == TokenType.LEFT_PAREN) {
                    depth++;
                    token = lexer.next();
                }
                if (token.type() != TokenType.IDENTIFIER) {
                    return false;
                }
                token = lexer.next();
                while (depth > 0) {
                    if (token.type() == TokenType.CAST) {
                        token = lexer.next();
                        if (token.type() != TokenType.IDENTIFIER) {
                            return false;
                        }
                        do {
                            token = lexer.next();
                        } while (token.type() == TokenType.IDENTIFIER);
                    } else if (token.type() == TokenType.RIGHT_PAREN) {
                        if (--depth == 0) {
                            return true;
                        }
                        token = lexer.next();
                    } else {
                        return false;
                    }
                }
                return false;
            } finally {
                lexer.index = savedIndex;
            }
        }

        private String columnOperand() {
            String column;
            if (match(TokenType.LEFT_PAREN)) {
                column = columnOperand();
                expect(TokenType.RIGHT_PAREN);
            } else {
                column = canonicalColumnName(expect(TokenType.IDENTIFIER).text());
            }
            textCasts(columnTypes.get(column), false);
            return column;
        }

        private CheckPredicate parseColumnPredicate() {
            String column = columnOperand();
            DatabaseType type = columnTypes.get(column);
            if (match(TokenType.IS)) {
                boolean negated = match(TokenType.NOT);
                expect(TokenType.NULL);
                return negated ? CheckPredicate.isNotNull(column) : CheckPredicate.isNull(column);
            }
            if (match(TokenType.IN)) {
                return CheckPredicate.in(column, parenthesizedValues(type));
            }
            CheckPredicate.ComparisonOperator operator = comparisonOperator(current.type());
            advance();
            if (operator == CheckPredicate.ComparisonOperator.EQUAL && match(TokenType.ANY)) {
                return CheckPredicate.in(column, postgresArrayValues(type));
            }
            return CheckPredicate.compare(column, operator, literal(type));
        }

        private List<Object> parenthesizedValues(DatabaseType type) {
            expect(TokenType.LEFT_PAREN);
            List<Object> values = values(type, TokenType.RIGHT_PAREN);
            expect(TokenType.RIGHT_PAREN);
            return values;
        }

        private List<Object> postgresArrayValues(DatabaseType type) {
            expect(TokenType.LEFT_PAREN);
            List<Object> values = postgresArrayOperand(type);
            expect(TokenType.RIGHT_PAREN);
            return values;
        }

        private List<Object> postgresArrayOperand(DatabaseType type) {
            List<Object> values;
            if (match(TokenType.LEFT_PAREN)) {
                values = postgresArrayOperand(type);
                expect(TokenType.RIGHT_PAREN);
            } else {
                expect(TokenType.ARRAY);
                expect(TokenType.LEFT_BRACKET);
                values = values(type, TokenType.RIGHT_BRACKET);
                expect(TokenType.RIGHT_BRACKET);
            }
            textCasts(type, true);
            return values;
        }

        private List<Object> values(DatabaseType type, TokenType end) {
            List<Object> values = new ArrayList<>();
            values.add(literal(type));
            while (match(TokenType.COMMA)) {
                values.add(literal(type));
            }
            if (current.type() != end) {
                throw unsupported();
            }
            return List.copyOf(values);
        }

        private Object literal(DatabaseType type) {
            List<String> casts = new ArrayList<>();
            Object value = literal(type, casts);
            requireLosslessCasts(value, type, casts);
            return value;
        }

        private Object literal(DatabaseType type, List<String> casts) {
            Object value;
            if (match(TokenType.LEFT_PAREN)) {
                value = literal(type, casts);
                expect(TokenType.RIGHT_PAREN);
            } else {
                Token token = current;
                advance();
                value = switch (token.type()) {
                    case STRING -> convertString(token.text(), type.logicalType());
                    case DATE_LITERAL -> oracleTemporalLiteral("DATE", token.text(), type);
                    case TIMESTAMP_LITERAL -> oracleTemporalLiteral("TIMESTAMP", token.text(), type);
                    case NUMBER -> checkNumber(token.text(), type, dialect);
                    case TRUE -> true;
                    case FALSE -> false;
                    default -> throw unsupported();
                };
            }
            if (dialect == InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL
                    && binaryTextType(type)) {
                textCasts(type, false);
            } else {
                literalCasts(casts);
            }
            return value;
        }

        /** Only erase PostgreSQL text/varchar casts that preserve the operand's value and comparison. */
        private void textCasts(DatabaseType type, boolean array) {
            while (match(TokenType.CAST)) {
                if (dialect != InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL
                        || !binaryTextType(type)) {
                    throw unsupported();
                }
                String cast = expect(TokenType.IDENTIFIER).text().toLowerCase(Locale.ROOT);
                if ("character".equals(cast)) {
                    if (!"varying".equalsIgnoreCase(expect(TokenType.IDENTIFIER).text())) {
                        throw unsupported();
                    }
                } else if (!"text".equals(cast) && !"varchar".equals(cast)
                        && !"pg_catalog.text".equals(cast) && !"pg_catalog.varchar".equals(cast)) {
                    throw unsupported();
                }
                if (array) {
                    expect(TokenType.LEFT_BRACKET);
                    expect(TokenType.RIGHT_BRACKET);
                }
            }
        }

        private static boolean binaryTextType(DatabaseType type) {
            return !type.isArray() && switch (type.baseName()) {
                case "TEXT", "VARCHAR", "CHARACTER VARYING", "PG_CATALOG.TEXT", "PG_CATALOG.VARCHAR" -> true;
                default -> false;
            };
        }

        private void literalCasts(List<String> casts) {
            while (match(TokenType.CAST)) {
                if (dialect != InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL) throw unsupported();
                StringBuilder cast = new StringBuilder(expect(TokenType.IDENTIFIER).text());
                while (current.type() == TokenType.IDENTIFIER) cast.append(' ').append(expect(TokenType.IDENTIFIER).text());
                casts.add(cast.toString());
            }
        }

        private String canonicalColumnName(String column) {
            if (columnTypes.containsKey(column)) {
                return column;
            }
            String match = null;
            for (Map.Entry<String, DatabaseType> entry : columnTypes.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(column)) {
                    if (match != null) {
                        throw new IllegalStateException("check column name is ambiguous");
                    }
                    match = entry.getKey();
                }
            }
            if (match == null) {
                throw new IllegalStateException("check references an unknown column");
            }
            return match;
        }

        private CheckPredicate logical(CheckPredicate.LogicalOperator operator,
                                       List<CheckPredicate> children) {
            if (children.size() == 1) {
                return children.getFirst();
            }
            if (operator == CheckPredicate.LogicalOperator.AND && children.size() == 2) {
                CheckPredicate range = range(children.get(0), children.get(1));
                if (range != null) {
                    return range;
                }
            }
            return new CheckPredicate.Logical(operator, children);
        }

        private static CheckPredicate range(CheckPredicate first, CheckPredicate second) {
            if (!(first instanceof CheckPredicate.Comparison lower)
                    || !(second instanceof CheckPredicate.Comparison upper)
                    || !lower.column().equals(upper.column())) {
                return null;
            }
            boolean lowerInclusive;
            boolean upperInclusive;
            if (lower.operator() == CheckPredicate.ComparisonOperator.GREATER_THAN) {
                lowerInclusive = false;
            } else if (lower.operator() == CheckPredicate.ComparisonOperator.GREATER_THAN_OR_EQUAL) {
                lowerInclusive = true;
            } else {
                return null;
            }
            if (upper.operator() == CheckPredicate.ComparisonOperator.LESS_THAN) {
                upperInclusive = false;
            } else if (upper.operator() == CheckPredicate.ComparisonOperator.LESS_THAN_OR_EQUAL) {
                upperInclusive = true;
            } else {
                return null;
            }
            return CheckPredicate.range(
                    lower.column(), lower.value(), lowerInclusive, upper.value(), upperInclusive);
        }

        private static CheckPredicate.ComparisonOperator comparisonOperator(TokenType token) {
            return switch (token) {
                case EQUAL -> CheckPredicate.ComparisonOperator.EQUAL;
                case NOT_EQUAL -> CheckPredicate.ComparisonOperator.NOT_EQUAL;
                case LESS -> CheckPredicate.ComparisonOperator.LESS_THAN;
                case LESS_EQUAL -> CheckPredicate.ComparisonOperator.LESS_THAN_OR_EQUAL;
                case GREATER -> CheckPredicate.ComparisonOperator.GREATER_THAN;
                case GREATER_EQUAL -> CheckPredicate.ComparisonOperator.GREATER_THAN_OR_EQUAL;
                default -> throw unsupported();
            };
        }

        private boolean match(TokenType type) {
            if (current.type() != type) {
                return false;
            }
            advance();
            return true;
        }

        private Token expect(TokenType type) {
            if (current.type() != type) {
                throw unsupported();
            }
            Token token = current;
            advance();
            return token;
        }

        private void advance() {
            current = lexer.next();
        }

        private void requireEnd() {
            expect(TokenType.END);
        }
    }

    private static final class Lexer {

        private final String source;
        private final InformationSchemaFormMetadataReader.SnapshotDialect dialect;
        private int index;

        private Lexer(String source, InformationSchemaFormMetadataReader.SnapshotDialect dialect) {
            this.source = Objects.requireNonNull(source, "check expression must not be null");
            this.dialect = Objects.requireNonNull(dialect, "snapshot dialect must not be null");
        }

        private Token next() {
            skipWhitespace();
            if (index >= source.length()) {
                return new Token(TokenType.END, "");
            }
            char value = source.charAt(index);
            if (dialect == InformationSchemaFormMetadataReader.SnapshotDialect.ORACLE
                    && (value == 'D' || value == 'd' || value == 'T' || value == 't')) {
                var temporal = ORACLE_TEMPORAL_LITERAL.matcher(source).region(index, source.length());
                if (temporal.lookingAt()) {
                    index = temporal.end();
                    return new Token("DATE".equalsIgnoreCase(temporal.group(1))
                            ? TokenType.DATE_LITERAL : TokenType.TIMESTAMP_LITERAL, temporal.group(2));
                }
            }
            if (dialect == InformationSchemaFormMetadataReader.SnapshotDialect.H2
                    && (value == 'C' || value == 'c')) {
                // H2 会给数值常量补 CAST；只识别当前词法位置，不改写字符串或引号内的标识符。
                var cast = H2_NUMERIC_LITERAL_CAST.matcher(source).region(index, source.length());
                if (cast.lookingAt()) {
                    index = cast.end();
                    return new Token(TokenType.NUMBER, cast.group(1));
                }
            }
            if (dialect == InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER) {
                if (value == '[') {
                    return identifier(']');
                }
                if ((value == 'N' || value == 'n') && index + 1 < source.length()
                        && source.charAt(index + 1) == '\'') {
                    index++;
                    return string();
                }
            }
            if (dialect == InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL
                    && source.regionMatches(true, index, "_utf8mb4", 0, 8)) {
                index += 8;
                skipWhitespace();
                if (index < source.length() && source.charAt(index) == '\'') {
                    return mysqlString();
                }
                if (index + 1 < source.length() && (source.charAt(index) == 'X'
                        || source.charAt(index) == 'x') && source.charAt(index + 1) == '\'') {
                    index++;
                    Token hex = string();
                    try {
                        return new Token(TokenType.STRING, new String(
                                HexFormat.of().parseHex(hex.text()), StandardCharsets.UTF_8));
                    } catch (IllegalArgumentException error) {
                        throw new IllegalStateException("invalid UTF-8 hexadecimal schema literal", error);
                    }
                }
                throw unsupported();
            }
            if (value == '\'' ) {
                return dialect == InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL
                        ? mysqlString() : string();
            }
            if (value == '"' || value == '`') {
                return identifier(value);
            }
            if (Character.isDigit(value) || ((value == '-' || value == '+')
                    && index + 1 < source.length() && Character.isDigit(source.charAt(index + 1)))) {
                return numberToken();
            }
            if (Character.isLetter(value) || value == '_') {
                return word();
            }
            index++;
            return switch (value) {
                case '(' -> new Token(TokenType.LEFT_PAREN, "(");
                case ')' -> new Token(TokenType.RIGHT_PAREN, ")");
                case '[' -> new Token(TokenType.LEFT_BRACKET, "[");
                case ']' -> new Token(TokenType.RIGHT_BRACKET, "]");
                case ',' -> new Token(TokenType.COMMA, ",");
                case '=' -> new Token(TokenType.EQUAL, "=");
                case '<' -> paired('<');
                case '>' -> paired('>');
                case ':' -> cast();
                default -> throw unsupported();
            };
        }

        private Token paired(char first) {
            if (index < source.length()) {
                char second = source.charAt(index);
                if (first == '<' && second == '>') {
                    index++;
                    return new Token(TokenType.NOT_EQUAL, "<>");
                }
                if (second == '=') {
                    index++;
                    return new Token(first == '<' ? TokenType.LESS_EQUAL : TokenType.GREATER_EQUAL,
                                     first + "=");
                }
            }
            return new Token(first == '<' ? TokenType.LESS : TokenType.GREATER,
                             Character.toString(first));
        }

        private Token cast() {
            if (dialect != InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL
                    || index >= source.length() || source.charAt(index) != ':') {
                throw unsupported();
            }
            index++;
            return new Token(TokenType.CAST, "::");
        }

        private Token string() {
            int start = index;
            int end = quotedStringEnd(source, start);
            index = end + 1;
            return new Token(TokenType.STRING, unquoteString(source.substring(start, index)));
        }

        private Token mysqlString() {
            // MySQL stores CHECK text using String::print escapes, independent of session sql_mode.
            index++;
            StringBuilder value = new StringBuilder();
            while (index < source.length()) {
                char current = source.charAt(index++);
                if (current == '\\') {
                    if (index == source.length()) {
                        throw unsupported();
                    }
                    value.append(switch (source.charAt(index++)) {
                        case '\\' -> '\\';
                        case '\'' -> '\'';
                        case '0' -> '\0';
                        case 'n' -> '\n';
                        case 'r' -> '\r';
                        case 'Z' -> '\u001a';
                        default -> throw unsupported();
                    });
                } else if (current != '\'') {
                    value.append(current);
                } else if (index < source.length() && source.charAt(index) == '\'') {
                    value.append('\'');
                    index++;
                } else {
                    return new Token(TokenType.STRING, value.toString());
                }
            }
            throw new IllegalStateException("unterminated schema string literal");
        }

        private Token identifier(char quote) {
            index++;
            StringBuilder value = new StringBuilder();
            while (index < source.length()) {
                char current = source.charAt(index++);
                if (current != quote) {
                    value.append(current);
                } else if (index < source.length() && source.charAt(index) == quote) {
                    value.append(quote);
                    index++;
                } else {
                    return new Token(TokenType.IDENTIFIER, value.toString());
                }
            }
            throw new IllegalStateException("unterminated schema identifier");
        }

        private Token numberToken() {
            int start = index++;
            while (index < source.length()) {
                char current = source.charAt(index);
                if (!Character.isDigit(current) && current != '.' && current != 'e' && current != 'E'
                        && current != '+' && current != '-') {
                    break;
                }
                index++;
            }
            return new Token(TokenType.NUMBER, source.substring(start, index));
        }

        private Token word() {
            int start = index++;
            while (index < source.length()) {
                char current = source.charAt(index);
                if (!Character.isLetterOrDigit(current) && current != '_' && current != '.') {
                    break;
                }
                index++;
            }
            String value = source.substring(start, index);
            return new Token(switch (value.toUpperCase(Locale.ROOT)) {
                case "TRUE" -> TokenType.TRUE;
                case "FALSE" -> TokenType.FALSE;
                case "NULL" -> TokenType.NULL;
                case "AND" -> TokenType.AND;
                case "OR" -> TokenType.OR;
                case "NOT" -> TokenType.NOT;
                case "IN" -> TokenType.IN;
                case "IS" -> TokenType.IS;
                case "ANY" -> TokenType.ANY;
                case "ARRAY" -> TokenType.ARRAY;
                default -> TokenType.IDENTIFIER;
            }, value);
        }

        private void skipWhitespace() {
            while (index < source.length() && Character.isWhitespace(source.charAt(index))) {
                index++;
            }
        }
    }

    private static IllegalStateException unsupported() {
        return new IllegalStateException("check constraint cannot be represented safely");
    }
}
