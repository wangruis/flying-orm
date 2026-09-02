package com.flying.orm.core.type;

import com.flying.orm.core.internal.Names;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Immutable, syntax-aware database type declaration.
 *
 * <p>A declaration is normalized and parsed once when metadata enters the ORM. Consumers use the structured
 * properties instead of repeatedly guessing with substring checks. Unknown vendor types remain usable as
 * {@link LogicalType#OTHER}; unsafe or malformed declarations are never allowed to borrow the semantics of a
 * valid prefix.</p>
 *
 * @author wangr
 * @date 2026-08-24
 * @version v1.0
 */
public final class DatabaseType {

    private static final String ARGUMENTS = "\\((?:\\d+|max)(?:,\\d+)?\\)";
    private static final String INTERVAL_PRECISION = "\\(\\d+\\)";
    private static final String SIMPLE_NAME = "[a-z_][a-z0-9_]*(?:\\.[a-z_][a-z0-9_]*)?";
    private static final String MULTI_WORD_NAME =
            "(?:double precision|character varying|national character varying|bit varying|long raw)";
    private static final String TIME_ZONE_NAME =
            "time(?:stamp)?(?:" + ARGUMENTS + ")? (?:with(?: local)?|without) time zone";
    private static final String INTERVAL_NAME =
            "interval (?:year(?:" + INTERVAL_PRECISION + ")? to month"
                    + "|day(?:" + INTERVAL_PRECISION + ")? to second(?:" + INTERVAL_PRECISION + ")?)";
    private static final Pattern SAFE_DECLARATION = Pattern.compile(
            "(?:" + TIME_ZONE_NAME
                    + "|" + INTERVAL_NAME
                    + "|" + MULTI_WORD_NAME + "(?:" + ARGUMENTS + ")?"
                    + "|" + SIMPLE_NAME + "(?:" + ARGUMENTS + ")?"
                    + "(?: unsigned)?(?: zerofill)?)(?:\\[\\])*");
    private static final Pattern WHITESPACE = Pattern.compile("\\s+");
    private static final Pattern OPEN_PARENTHESIS_SPACE = Pattern.compile("\\s*\\(\\s*");
    private static final Pattern CLOSE_PARENTHESIS_SPACE = Pattern.compile("\\s*\\)");
    private static final Pattern COMMA_SPACE = Pattern.compile("\\s*,\\s*");
    private static final Pattern ARRAY_SPACE = Pattern.compile("\\s*\\[\\s*\\]");
    private static final Pattern ARGUMENT_GROUP = Pattern.compile("\\(([^()]*)\\)");

    private final String declaration;
    private final String canonical;
    private final String baseName;
    private final List<String> arguments;
    private final int arrayDimensions;
    private final boolean unsigned;
    private final boolean zerofill;
    private final boolean safeDeclaration;
    private final LogicalType logicalType;

    private DatabaseType(String declaration) {
        this.declaration = Names.requireText(declaration, "database type");
        this.canonical = canonicalize(this.declaration);
        this.safeDeclaration = SAFE_DECLARATION.matcher(canonical).matches();
        if (!safeDeclaration) {
            this.baseName = canonical.toUpperCase(Locale.ROOT);
            this.arguments = List.of();
            this.arrayDimensions = 0;
            this.unsigned = false;
            this.zerofill = false;
            this.logicalType = LogicalType.OTHER;
            return;
        }

        Parsed parsed = parse(canonical);
        this.baseName = parsed.baseName();
        this.arguments = parsed.arguments();
        this.arrayDimensions = parsed.arrayDimensions();
        this.unsigned = parsed.unsigned();
        this.zerofill = parsed.zerofill();
        this.logicalType = logicalType(baseName);
    }

    /** @param declaration database type declaration; @return parsed immutable type */
    public static DatabaseType of(String declaration) {
        return new DatabaseType(declaration);
    }

    /** @return trimmed declaration as supplied by the metadata boundary */
    public String declaration() {
        return declaration;
    }

    /** @return lower-case declaration with non-semantic whitespace removed */
    public String canonical() {
        return canonical;
    }

    /** @return upper-case type name without arguments, arrays or numeric modifiers */
    public String baseName() {
        return baseName;
    }

    /** @return immutable argument values from the declaration, without parentheses */
    public List<String> arguments() {
        return arguments;
    }

    /** @return number of trailing SQL array dimensions */
    public int arrayDimensions() {
        return arrayDimensions;
    }

    /** @return whether this is an array rather than a scalar declaration */
    public boolean isArray() {
        return arrayDimensions > 0;
    }

    /** @return whether MySQL unsigned semantics were declared, including ZEROFILL */
    public boolean unsigned() {
        return unsigned;
    }

    /** @return whether MySQL ZEROFILL was declared */
    public boolean zerofill() {
        return zerofill;
    }

    /** @return whether the whole declaration matches the supported safe SQL type grammar */
    public boolean safeDeclaration() {
        return safeDeclaration;
    }

    /**
     * Requires the declaration to be safe for direct SQL type rendering.
     *
     * @param fieldName caller-facing metadata name used in the stable error message
     * @return this parsed type
     */
    public DatabaseType requireSafe(String fieldName) {
        if (!safeDeclaration) {
            throw new IllegalArgumentException(Names.requireText(fieldName, "database type field name")
                                                       + " contains unsupported SQL type syntax");
        }
        return this;
    }

    /** @return canonical declaration with numeric argument groups removed, preserving modifiers and arrays */
    public String comparisonShape() {
        if (!safeDeclaration) {
            return canonical;
        }
        return WHITESPACE.matcher(ARGUMENT_GROUP.matcher(canonical).replaceAll("").trim()).replaceAll(" ");
    }

    /** @return exact database-independent meaning, or OTHER for unknown and malformed types */
    public LogicalType logicalType() {
        return logicalType;
    }

    /** @return true only for scalar character storage */
    public boolean isTextual() {
        return !isArray() && logicalType.textual();
    }

    /** @return true only for scalar binary storage */
    public boolean isBinary() {
        return !isArray() && logicalType.binary();
    }

    /** @return true only for scalar numeric storage */
    public boolean isNumeric() {
        return !isArray() && logicalType.numeric();
    }

    /** @return true only for scalar temporal storage */
    public boolean isTemporal() {
        return !isArray() && logicalType.temporal();
    }

    @Override
    public boolean equals(Object candidate) {
        return this == candidate
                || candidate instanceof DatabaseType other && canonical.equals(other.canonical);
    }

    @Override
    public int hashCode() {
        return canonical.hashCode();
    }

    @Override
    public String toString() {
        return declaration;
    }

    private static String canonicalize(String value) {
        String normalized = WHITESPACE.matcher(value.trim().toLowerCase(Locale.ROOT)).replaceAll(" ");
        normalized = OPEN_PARENTHESIS_SPACE.matcher(normalized).replaceAll("(");
        normalized = CLOSE_PARENTHESIS_SPACE.matcher(normalized).replaceAll(")");
        normalized = COMMA_SPACE.matcher(normalized).replaceAll(",");
        return ARRAY_SPACE.matcher(normalized).replaceAll("[]");
    }

    private static Parsed parse(String canonical) {
        String scalar = canonical;
        int arrays = 0;
        while (scalar.endsWith("[]")) {
            arrays++;
            scalar = scalar.substring(0, scalar.length() - 2).stripTrailing();
        }

        boolean zerofill = scalar.endsWith(" zerofill");
        if (zerofill) {
            scalar = scalar.substring(0, scalar.length() - " zerofill".length()).stripTrailing();
        }
        boolean unsigned = zerofill || scalar.endsWith(" unsigned");
        if (scalar.endsWith(" unsigned")) {
            scalar = scalar.substring(0, scalar.length() - " unsigned".length()).stripTrailing();
        }

        List<String> arguments = new ArrayList<>(2);
        Matcher matcher = ARGUMENT_GROUP.matcher(scalar);
        StringBuilder base = new StringBuilder(scalar.length());
        int copiedUntil = 0;
        while (matcher.find()) {
            base.append(scalar, copiedUntil, matcher.start());
            for (String argument : matcher.group(1).split(",", -1)) {
                arguments.add(argument);
            }
            copiedUntil = matcher.end();
        }
        base.append(scalar, copiedUntil, scalar.length());
        String baseName = WHITESPACE.matcher(base.toString().trim()).replaceAll(" ").toUpperCase(Locale.ROOT);
        return new Parsed(baseName, List.copyOf(arguments), arrays, unsigned, zerofill);
    }

    private static LogicalType logicalType(String baseName) {
        return switch (baseName) {
            case "TINYINT", "SMALLINT", "INT2", "MEDIUMINT", "SMALLSERIAL", "SERIAL2" ->
                    LogicalType.SMALL_INTEGER;
            case "INT", "INTEGER", "INT4", "SERIAL", "SERIAL4" -> LogicalType.INTEGER;
            case "BIGINT", "INT8", "BIGSERIAL", "SERIAL8" -> LogicalType.BIG_INTEGER;
            case "DEC", "DECIMAL", "NUMERIC", "NUMBER", "MONEY", "SMALLMONEY" -> LogicalType.DECIMAL;
            case "FLOAT", "REAL", "DOUBLE", "DOUBLE PRECISION", "FLOAT4", "FLOAT8",
                    "BINARY_FLOAT", "BINARY_DOUBLE" -> LogicalType.FLOAT;
            case "BOOL", "BOOLEAN" -> LogicalType.BOOLEAN;
            case "CHAR", "CHARACTER", "CHARACTER VARYING", "NATIONAL CHARACTER VARYING", "VARCHAR",
                    "VARCHAR2", "NCHAR", "NVARCHAR", "NVARCHAR2", "TEXT", "TINYTEXT", "MEDIUMTEXT",
                    "LONGTEXT", "CLOB", "NCLOB", "BPCHAR", "NTEXT" -> LogicalType.TEXT;
            case "BINARY", "VARBINARY", "LONGVARBINARY", "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB",
                    "MYSQL_BINARY", "MYSQL_BLOB", "BYTEA", "RAW", "LONG RAW", "IMAGE",
                    "PROTECTED_BINARY", "PROTECTED_HASH" ->
                    LogicalType.BINARY;
            case "DATE" -> LogicalType.DATE;
            case "TIME", "TIME WITHOUT TIME ZONE" -> LogicalType.TIME;
            case "TIME WITH TIME ZONE", "OFFSET_TIME", "TIMETZ" -> LogicalType.OFFSET_TIME;
            case "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE",
                    "DATETIME", "DATETIME2", "ORACLE_DATE",
                    "SMALLDATETIME", "SQLSERVER_DATETIME", "SQLSERVER_SMALLDATETIME" -> LogicalType.TIMESTAMP;
            case "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE", "DATETIMEOFFSET" -> LogicalType.OFFSET_TIMESTAMP;
            case "JSON", "JSONB" -> LogicalType.JSON;
            case "UUID", "UNIQUEIDENTIFIER" -> LogicalType.UUID;
            case "XML", "SQLXML" -> LogicalType.XML;
            case "VECTOR" -> LogicalType.VECTOR;
            default -> baseName.startsWith("INTERVAL ") ? LogicalType.INTERVAL : LogicalType.OTHER;
        };
    }

    private record Parsed(String baseName,
                          List<String> arguments,
                          int arrayDimensions,
                          boolean unsigned,
                          boolean zerofill) {

        private Parsed {
            Objects.requireNonNull(baseName, "database type base name must not be null");
            arguments = List.copyOf(arguments);
        }
    }
}
