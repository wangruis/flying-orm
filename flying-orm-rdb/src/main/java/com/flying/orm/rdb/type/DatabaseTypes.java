package com.flying.orm.rdb.type;

import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.core.type.LogicalType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Explicit database-driver and metadata policies layered on Core's shared database type model.
 *
 * <p>General syntax and meaning belong to {@link DatabaseType}. This class contains only behavior that genuinely
 * changes by database: driver binding exceptions and the five metadata-to-logical declaration maps.</p>
 *
 * @author wangr
 * @date 2026-08-24
 * @version v1.0
 */
public final class DatabaseTypes {

    private DatabaseTypes() {
    }

    /** @return whether the scalar codec has an explicit conversion contract for this type */
    public static boolean supportsScalar(DatabaseType type, String dialectName) {
        DatabaseType safeType = requireType(type);
        if (safeType.isArray()) {
            return false;
        }
        return switch (effectiveLogicalType(safeType, dialectName)) {
            case SMALL_INTEGER, INTEGER, BIG_INTEGER, DECIMAL, FLOAT, BOOLEAN, DATE, TIME,
                    TIMESTAMP, OFFSET_TIMESTAMP -> true;
            default -> false;
        };
    }

    /**
     * Returns the Java type expected by the driver for one scalar value. Unknown types deliberately stay
     * {@link Object} so custom codecs and the driver retain ownership.
     */
    public static Class<?> parameterType(DatabaseType type, String dialectName, boolean nativeBoolean) {
        DatabaseType safeType = requireType(type);
        String dialect = normalizeDialect(dialectName);
        if (safeType.isArray()) {
            return Object.class;
        }
        return switch (effectiveLogicalType(safeType, dialect)) {
            case BIG_INTEGER -> safeType.unsigned() ? BigInteger.class : Long.class;
            case INTEGER -> safeType.unsigned() ? Long.class : Integer.class;
            case SMALL_INTEGER -> Integer.class;
            case DECIMAL -> BigDecimal.class;
            case FLOAT -> Double.class;
            case BOOLEAN -> legacyOracleBoolean(dialect, nativeBoolean) ? Integer.class : Boolean.class;
            case OFFSET_TIMESTAMP -> "MYSQL".equals(dialect) ? LocalDateTime.class : OffsetDateTime.class;
            case TIMESTAMP -> LocalDateTime.class;
            case DATE -> LocalDate.class;
            case TIME -> "ORACLE".equals(dialect) ? String.class : LocalTime.class;
            case INTERVAL -> oracleIntervalParameterType(safeType, dialect);
            default -> Object.class;
        };
    }

    /** @return logical meaning after applying the few dialect-specific driver rules */
    public static LogicalType effectiveLogicalType(DatabaseType type, String dialectName) {
        DatabaseType safeType = requireType(type);
        if (safeType.isArray()) {
            return LogicalType.OTHER;
        }
        String dialect = normalizeDialect(dialectName);
        if ("MYSQL".equals(dialect)
                && "BIT".equals(safeType.baseName())
                && safeType.arguments().equals(List.of("1"))) {
            return LogicalType.BOOLEAN;
        }
        return safeType.logicalType();
    }

    /** @return whether an offset timestamp follows MySQL's UTC LocalDateTime driver contract */
    public static boolean mysqlOffsetTimestamp(DatabaseType type, String dialectName) {
        return effectiveLogicalType(type, dialectName) == LogicalType.OFFSET_TIMESTAMP
                && "MYSQL".equals(normalizeDialect(dialectName));
    }

    /** @return whether logical boolean storage is NUMBER(1) rather than native SQL BOOLEAN */
    public static boolean legacyOracleBoolean(String dialectName, boolean nativeBoolean) {
        return "ORACLE".equals(normalizeDialect(dialectName)) && !nativeBoolean;
    }

    /**
     * Converts one driver metadata declaration to flying-orm's stable cross-dialect declaration.
     * Unknown vendor types retain their canonical physical name instead of being guessed from substrings.
     */
    public static String logicalDeclaration(String declaration, String dialectName) {
        DatabaseType type = DatabaseType.of(declaration);
        return switch (normalizeDialect(dialectName)) {
            case "H2" -> h2(type);
            case "MYSQL" -> mysql(type);
            case "POSTGRESQL" -> postgresql(type);
            case "ORACLE" -> oracle(type);
            case "SQLSERVER", "SQL_SERVER" -> sqlServer(type);
            default -> upper(type);
        };
    }

    /**
     * Returns the fixed PostgreSQL cast used by supported one-dimensional array conditions.
     * The declaration is parsed before this method is called; user text is never copied into SQL.
     */
    public static String postgresqlArrayCast(DatabaseType type) {
        DatabaseType arrayType = requireType(type).requireSafe("array condition data type");
        if (arrayType.arrayDimensions() != 1) {
            throw new IllegalArgumentException("array conditions require a one-dimensional SQL array type");
        }
        return switch (arrayType.baseName()) {
            case "BIGINT", "INT8", "BIGSERIAL" -> "bigint[]";
            case "SMALLINT", "INT2" -> "smallint[]";
            case "INTEGER", "INT", "INT4", "SERIAL" -> "integer[]";
            case "DECIMAL", "NUMERIC" -> "numeric[]";
            case "DOUBLE", "DOUBLE PRECISION", "FLOAT", "FLOAT8" -> "double precision[]";
            case "REAL", "FLOAT4" -> "real[]";
            case "BOOLEAN", "BOOL" -> "boolean[]";
            case "DATE" -> "date[]";
            case "TIME", "TIME WITHOUT TIME ZONE" -> "time[]";
            case "TIME WITH TIME ZONE", "OFFSET_TIME", "TIMETZ" -> "time with time zone[]";
            case "TIMESTAMP", "DATETIME", "TIMESTAMP WITHOUT TIME ZONE" -> "timestamp[]";
            case "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ" -> "timestamp with time zone[]";
            case "UUID" -> "uuid[]";
            case "VARCHAR", "CHARACTER VARYING" -> "varchar[]";
            case "CHAR", "CHARACTER", "BPCHAR" -> "character[]";
            case "TEXT" -> "text[]";
            default -> throw new IllegalArgumentException("unsupported PostgreSQL array condition type");
        };
    }

    private static String h2(DatabaseType type) {
        String logical = switch (type.baseName()) {
            case "CHARACTER VARYING", "VARCHAR", "VARCHAR_IGNORECASE" -> "VARCHAR";
            case "CHARACTER LARGE OBJECT", "CLOB" -> "CLOB";
            case "BINARY LARGE OBJECT", "BLOB" -> "BLOB";
            case "DECIMAL", "NUMERIC" -> "DECIMAL";
            case "INT", "INTEGER" -> "INTEGER";
            case "BIGINT" -> "BIGINT";
            case "BOOLEAN" -> "BOOLEAN";
            case "TIMESTAMP WITH TIME ZONE" -> "TIMESTAMPTZ";
            case "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE" -> "TIMESTAMP";
            case "DATE" -> "DATE";
            case "TIME WITH TIME ZONE" -> "OFFSET_TIME";
            case "TIME", "TIME WITHOUT TIME ZONE" -> "TIME";
            default -> null;
        };
        return mapped(type, logical);
    }

    private static String mysql(DatabaseType type) {
        if (integer(type)) {
            if ("TINYINT".equals(type.baseName())
                    && type.arguments().equals(List.of("1"))
                    && !type.unsigned()
                    && !type.zerofill()) {
                return "TINYINT(1)";
            }
            String logical = "INTEGER".equals(type.baseName()) ? "INT" : type.baseName();
            return logical + (type.unsigned() ? " UNSIGNED" : "") + (type.zerofill() ? " ZEROFILL" : "");
        }
        if ("BINARY".equals(type.baseName()) && !type.arguments().isEmpty()) {
            return "MYSQL_BINARY";
        }
        String logical = switch (type.baseName()) {
            case "VARCHAR", "CHAR" -> "VARCHAR";
            case "TEXT", "TINYTEXT", "MEDIUMTEXT" -> "TEXT";
            case "LONGTEXT" -> "LONGTEXT";
            case "TINYBLOB" -> "TINYBLOB";
            case "BLOB" -> "MYSQL_BLOB";
            case "MEDIUMBLOB" -> "MEDIUMBLOB";
            case "LONGBLOB" -> "BLOB";
            case "DECIMAL", "NUMERIC" -> "DECIMAL";
            case "BOOLEAN", "BOOL" -> "BOOLEAN";
            case "BIT" -> "BIT";
            case "TIMESTAMP" -> "TIMESTAMPTZ";
            case "DATETIME" -> "TIMESTAMP";
            case "DATE" -> "DATE";
            case "TIME" -> "TIME";
            default -> null;
        };
        return mapped(type, logical);
    }

    private static String postgresql(DatabaseType type) {
        String logical;
        if (type.isArray() && ("SMALLINT".equals(type.baseName()) || "INT2".equals(type.baseName()))) {
            logical = "SMALLINT";
        } else if (type.isArray()
                && ("TIME WITH TIME ZONE".equals(type.baseName()) || "TIMETZ".equals(type.baseName()))) {
            logical = "TIMETZ";
        } else {
            logical = switch (type.baseName()) {
                case "CHARACTER VARYING", "CHARACTER", "VARCHAR", "CHAR" -> "VARCHAR";
                case "TEXT" -> "TEXT";
                case "BYTEA" -> "BLOB";
                case "NUMERIC", "DECIMAL" -> "DECIMAL";
                case "INTEGER", "INT", "INT4", "SMALLINT", "INT2" -> "INTEGER";
                case "BIGINT", "INT8" -> "BIGINT";
                case "BOOLEAN", "BOOL" -> "BOOLEAN";
                case "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ" -> "TIMESTAMPTZ";
                case "TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP" -> "TIMESTAMP";
                case "DATE" -> "DATE";
                case "TIME WITH TIME ZONE" -> "OFFSET_TIME";
                case "TIME WITHOUT TIME ZONE", "TIME" -> "TIME";
                default -> null;
            };
        }
        return mapped(type, logical);
    }

    private static String oracle(DatabaseType type) {
        return switch (type.baseName()) {
            case "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR" -> "VARCHAR";
            case "CLOB" -> "CLOB";
            case "NCLOB" -> "NCLOB";
            case "LONG" -> "TEXT";
            case "BLOB", "RAW", "LONG RAW" -> "BLOB";
            case "NUMBER", "DECIMAL", "NUMERIC", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE" -> "DECIMAL";
            case "TIMESTAMP WITH TIME ZONE" -> "TIMESTAMPTZ";
            case "TIMESTAMP WITH LOCAL TIME ZONE" -> "TIMESTAMP WITH LOCAL TIME ZONE";
            case "TIMESTAMP" -> "TIMESTAMP";
            case "DATE" -> "ORACLE_DATE";
            case "INTERVAL DAY TO SECOND", "INTERVAL YEAR TO MONTH" -> upper(type);
            default -> type.baseName();
        };
    }

    private static Class<?> oracleIntervalParameterType(DatabaseType type, String dialect) {
        if (!"ORACLE".equals(dialect)) {
            return Object.class;
        }
        return switch (type.baseName()) {
            case "INTERVAL DAY TO SECOND" -> Duration.class;
            case "INTERVAL YEAR TO MONTH" -> Period.class;
            default -> Object.class;
        };
    }

    private static String sqlServer(DatabaseType type) {
        if ("NVARCHAR".equals(type.baseName()) && type.arguments().equals(List.of("max"))) {
            return "TEXT";
        }
        if ("VARCHAR".equals(type.baseName()) && type.arguments().equals(List.of("max"))) {
            return "VARCHAR(MAX)";
        }
        return switch (type.baseName()) {
            case "VARCHAR", "NVARCHAR", "CHAR", "NCHAR", "UNIQUEIDENTIFIER" -> "VARCHAR";
            case "TEXT", "NTEXT", "XML" -> "TEXT";
            case "BINARY", "VARBINARY", "IMAGE" -> "BLOB";
            case "DECIMAL", "NUMERIC", "MONEY", "SMALLMONEY", "FLOAT", "REAL" -> "DECIMAL";
            case "INT", "SMALLINT", "TINYINT" -> "INTEGER";
            case "BIGINT" -> "BIGINT";
            case "BIT" -> "BOOLEAN";
            case "TIMESTAMP", "ROWVERSION" -> "ROWVERSION";
            case "DATETIMEOFFSET" -> "TIMESTAMPTZ";
            case "DATETIME2" -> "TIMESTAMP";
            case "DATETIME" -> "SQLSERVER_DATETIME";
            case "SMALLDATETIME" -> "SQLSERVER_SMALLDATETIME";
            case "DATE" -> "DATE";
            case "TIME" -> "TIME";
            default -> upper(type);
        };
    }

    private static boolean integer(DatabaseType type) {
        return switch (type.baseName()) {
            case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT" -> true;
            default -> false;
        };
    }

    private static String mapped(DatabaseType type, String logical) {
        return logical == null ? upper(type) : logical + arguments(type) + arrays(type);
    }

    private static String arguments(DatabaseType type) {
        return type.arguments().isEmpty() ? "" : "(" + String.join(",", type.arguments()) + ")";
    }

    private static String arrays(DatabaseType type) {
        return "[]".repeat(type.arrayDimensions());
    }

    private static String upper(DatabaseType type) {
        return type.canonical().toUpperCase(Locale.ROOT);
    }

    private static DatabaseType requireType(DatabaseType type) {
        return Objects.requireNonNull(type, "database type must not be null");
    }

    private static String normalizeDialect(String value) {
        return Objects.requireNonNull(value, "dialect name must not be null").trim().toUpperCase(Locale.ROOT);
    }
}
