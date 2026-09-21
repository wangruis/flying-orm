package com.flying.orm.rdb.schema;

import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.core.type.LogicalType;

import java.util.List;
import java.util.Locale;
import java.util.Set;

/** Compares physical DDL type shapes without changing their runtime meaning. */
final class SchemaTypeComparison {

    private static final String POSTGRESQL_VECTOR_EXTENSION_MARKER = "_flying_orm_pgvector_";

    private static final Set<String> MYSQL_INTEGER_TYPES =
            Set.of("TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT");
    private static final Set<String> POSTGRESQL_DEFAULT_PRECISION_TYPES = Set.of(
            "time", "timetz", "timestamp", "timestamptz");
    private static final Set<String> POSTGRESQL_CATALOG_TYPES = Set.of(
            "aclitem", "bit", "bool", "box", "bpchar", "bytea", "cid", "cidr", "circle",
            "date", "daterange", "datemultirange", "float4", "float8", "inet", "int2",
            "int2vector", "int4", "int4range", "int4multirange", "int8", "int8range",
            "int8multirange", "interval", "json", "jsonb", "jsonpath", "line", "lseg",
            "macaddr", "macaddr8", "money", "name", "numeric", "nummultirange", "numrange",
            "oid", "oidvector", "path", "pg_lsn", "pg_snapshot", "point", "polygon",
            "refcursor", "regclass", "regcollation", "regconfig", "regdictionary", "regnamespace",
            "regoper", "regoperator", "regproc", "regprocedure", "regrole", "regtype", "text",
            "tid", "time", "timestamp", "timestamptz", "timetz", "tsmultirange", "tsquery",
            "tsrange", "tstzmultirange", "tstzrange", "tsvector", "txid_snapshot", "uuid",
            "varbit", "varchar", "xid", "xid8", "xml");
    private static final Set<String> SQL_SERVER_DEFAULT_PRECISION_TYPES = Set.of(
            "time", "datetime2", "datetimeoffset");

    private final SchemaDialect.GeneratedValueStyle databaseStyle;

    SchemaTypeComparison(SchemaDialect.GeneratedValueStyle databaseStyle) {
        this.databaseStyle = databaseStyle;
    }

    boolean same(String left, String right) {
        if (databaseStyle == SchemaDialect.GeneratedValueStyle.POSTGRESQL) {
            DatabaseType leftType = DatabaseType.of(left).requireSafe("data type");
            DatabaseType rightType = DatabaseType.of(right).requireSafe("data type");
            String leftSchema = pgVectorSchema(leftType);
            String rightSchema = pgVectorSchema(rightType);
            if (leftSchema != null || rightSchema != null) {
                if (!sameVectorShape(leftType, rightType)) {
                    return false;
                }
                if (leftSchema != null && rightSchema != null) {
                    return leftSchema.equals(rightSchema);
                }
                String markerSchema = leftSchema == null ? rightSchema : leftSchema;
                DatabaseType candidate = leftSchema == null ? leftType : rightType;
                String base = candidate.baseName().toLowerCase(Locale.ROOT);
                return "vector".equals(base) || (markerSchema + ".vector").equals(base);
            }
        }
        return comparable(left).equals(comparable(right));
    }

    private String comparable(String value) {
        String type = comparableOracleNumeric(canonical(value));
        if (databaseStyle == SchemaDialect.GeneratedValueStyle.SQL_SERVER && "timestamp".equals(type)) {
            return "rowversion";
        }
        type = comparablePostgreSqlAlias(type);
        type = comparableH2Type(type);
        type = comparableMysqlBit(type);
        type = comparableMysqlInteger(type);
        DatabaseType parsed = DatabaseType.of(type).requireSafe("data type");
        if (parsed.arguments().size() != 1
                || !isDefaultTemporalPrecision(baseType(parsed), parsed.arguments().getFirst())) {
            return type;
        }
        return parsed.comparisonShape();
    }

    static String postgresqlComparable(String type) {
        DatabaseType parsed = DatabaseType.of(type).requireSafe("data type");
        String base = parsed.baseName().toLowerCase(Locale.ROOT);
        List<String> arguments = parsed.arguments();
        boolean catalog = base.startsWith("pg_catalog.");
        if (catalog) {
            String unqualified = base.substring("pg_catalog.".length());
            if (postgresqlCatalogBuiltIn(unqualified)) {
                base = unqualified;
            } else {
                catalog = false;
            }
        }
        String normalized = switch (base) {
            case "smallint", "int2" -> "smallint";
            case "integer", "int", "int4" -> "integer";
            case "bigint", "int8" -> "bigint";
            case "dec", "decimal", "numeric" -> "numeric";
            case "real", "float4" -> "real";
            case "double precision", "float8" -> "double precision";
            case "float" -> postgresqlFloat(arguments);
            case "boolean", "bool" -> "boolean";
            case "varchar", "character varying", "national character varying" -> "varchar";
            case "char", "character", "nchar" -> "char";
            case "bpchar" -> arguments.isEmpty() ? "bpchar" : "char";
            case "varbit", "bit varying" -> "varbit";
            case "timestamp", "timestamp without time zone" -> "timestamp";
            case "timestamptz", "timestamp with time zone" -> "timestamptz";
            case "time", "time without time zone" -> "time";
            case "timetz", "time with time zone" -> "timetz";
            default -> base;
        };
        if ("float".equals(base) && !"float".equals(normalized)) {
            arguments = List.of();
        } else if (("bit".equals(normalized) || "char".equals(normalized))
                && arguments.equals(List.of("1"))) {
            arguments = List.of();
        } else if ("numeric".equals(normalized) && arguments.size() == 2
                && "0".equals(arguments.get(1))) {
            arguments = List.of(arguments.getFirst());
        }
        int dimensions = parsed.isArray() ? 1 : 0;
        if (!catalog && normalized.equals(base)
                && arguments.equals(parsed.arguments()) && dimensions == parsed.arrayDimensions()) {
            return type;
        }
        String suffix = arguments.isEmpty() ? "" : "(" + String.join(",", arguments) + ")";
        return normalized + suffix + "[]".repeat(dimensions);
    }

    private static boolean postgresqlCatalogBuiltIn(String base) {
        return POSTGRESQL_CATALOG_TYPES.contains(base)
                || base.startsWith("interval ");
    }

    private static String postgresqlFloat(List<String> arguments) {
        if (arguments.isEmpty()) {
            return "double precision";
        }
        if (arguments.size() != 1) {
            return "float";
        }
        try {
            int precision = Integer.parseInt(arguments.getFirst());
            if (precision >= 1 && precision <= 24) {
                return "real";
            }
            return precision >= 25 && precision <= 53 ? "double precision" : "float";
        } catch (NumberFormatException ignored) {
            return "float";
        }
    }

    private String comparablePostgreSqlAlias(String type) {
        return databaseStyle == SchemaDialect.GeneratedValueStyle.POSTGRESQL
                ? postgresqlComparable(type) : type;
    }

    private static String pgVectorSchema(DatabaseType type) {
        String base = type.baseName().toLowerCase(Locale.ROOT);
        if (!base.startsWith(POSTGRESQL_VECTOR_EXTENSION_MARKER) || !base.endsWith(".vector")) {
            return null;
        }
        return base.substring(
                POSTGRESQL_VECTOR_EXTENSION_MARKER.length(), base.length() - ".vector".length());
    }

    private static boolean sameVectorShape(DatabaseType left, DatabaseType right) {
        return left.arguments().equals(right.arguments()) && left.isArray() == right.isArray();
    }

    private String comparableMysqlInteger(String type) {
        if (databaseStyle != SchemaDialect.GeneratedValueStyle.MYSQL) {
            return type;
        }
        DatabaseType parsed = DatabaseType.of(type).requireSafe("data type");
        if ("BOOLEAN".equals(parsed.baseName()) || "BOOL".equals(parsed.baseName())) {
            return "boolean";
        }
        if (!MYSQL_INTEGER_TYPES.contains(parsed.baseName())) {
            return type;
        }
        if (parsed.arguments().isEmpty()) {
            return normalizeMysqlIntegerAlias(type);
        }
        if (parsed.arguments().size() != 1 || !digits(parsed.arguments().getFirst())) {
            return type;
        }
        if ("TINYINT".equals(parsed.baseName())
                && "1".equals(parsed.arguments().getFirst())
                && !parsed.unsigned()
                && parsed.arrayDimensions() == 0) {
            return "boolean";
        }
        return normalizeMysqlIntegerAlias(parsed.comparisonShape());
    }

    private static boolean digits(String value) {
        for (int index = 0; index < value.length(); index++) {
            if (!Character.isDigit(value.charAt(index))) {
                return false;
            }
        }
        return !value.isEmpty();
    }

    private static String normalizeMysqlIntegerAlias(String type) {
        String normalized;
        if ("integer".equals(type)) {
            normalized = "int";
        } else if (type.startsWith("integer ")) {
            normalized = "int" + type.substring("integer".length());
        } else {
            normalized = type;
        }
        if (normalized.endsWith(" zerofill") && !normalized.contains(" unsigned")) {
            return normalized.substring(0, normalized.length() - " zerofill".length())
                    + " unsigned zerofill";
        }
        return normalized;
    }

    private String comparableMysqlBit(String type) {
        return databaseStyle == SchemaDialect.GeneratedValueStyle.MYSQL && "bit".equals(type)
                ? "bit(1)" : type;
    }

    private String comparableH2Type(String type) {
        if (databaseStyle != SchemaDialect.GeneratedValueStyle.H2) {
            return type;
        }
        return h2Comparable(type);
    }

    static String h2Comparable(String value) {
        String type = canonical(value);
        DatabaseType parsed = DatabaseType.of(type).requireSafe("data type");
        if (parsed.arguments().isEmpty()
                && ("TEXT".equals(parsed.baseName())
                    || "VARCHAR".equals(parsed.baseName())
                    || "CHARACTER VARYING".equals(parsed.baseName()))) {
            return "character varying(1000000000)";
        }
        String normalized = switch (parsed.baseName()) {
            case "VARCHAR" -> "character varying";
            case "BINARY LARGE OBJECT" -> "blob";
            case "CHARACTER LARGE OBJECT" -> "clob";
            case "DECIMAL" -> "numeric";
            default -> null;
        };
        return normalized == null ? type : normalized + type.substring(parsed.baseName().length());
    }

    private String comparableOracleNumeric(String type) {
        if (databaseStyle != SchemaDialect.GeneratedValueStyle.ORACLE) {
            return type;
        }
        if ("float(126)".equals(type)) {
            return "float";
        }
        DatabaseType parsed = DatabaseType.of(type).requireSafe("data type");
        if (!"NUMBER".equals(parsed.baseName())
                || parsed.arguments().size() != 2
                || !"0".equals(parsed.arguments().get(1))) {
            return type;
        }
        int open = type.indexOf('(');
        int close = type.indexOf(')', open + 1);
        return type.substring(0, open + 1) + parsed.arguments().getFirst() + type.substring(close);
    }

    private boolean isDefaultTemporalPrecision(String baseType, String argument) {
        return switch (databaseStyle) {
            case H2 -> ("0".equals(argument) && "time".equals(baseType))
                    || ("6".equals(argument) && "timestamp".equals(baseType));
            case MYSQL -> "0".equals(argument)
                    && ("time".equals(baseType) || "datetime".equals(baseType) || "timestamp".equals(baseType));
            case POSTGRESQL -> "6".equals(argument)
                    && POSTGRESQL_DEFAULT_PRECISION_TYPES.contains(baseType);
            case ORACLE -> "6".equals(argument) && "timestamp".equals(baseType);
            case SQL_SERVER -> "7".equals(argument)
                    && SQL_SERVER_DEFAULT_PRECISION_TYPES.contains(baseType);
            case NONE -> false;
        };
    }

    static boolean safeWidening(String current, String target) {
        DatabaseType currentType = DatabaseType.of(current).requireSafe("current data type");
        DatabaseType targetType = DatabaseType.of(target).requireSafe("target data type");
        if (currentType.logicalType() == LogicalType.INTERVAL
                || currentType.equals(targetType)
                || !currentType.comparisonShape().equals(targetType.comparisonShape())) {
            return false;
        }
        long[] currentArguments = numericArguments(currentType);
        long[] targetArguments = numericArguments(targetType);
        if (currentArguments.length == 0 || currentArguments.length != targetArguments.length) {
            return false;
        }
        for (int index = 0; index < currentArguments.length; index++) {
            if (currentArguments[index] < 0 || targetArguments[index] < 0) {
                return false;
            }
        }
        if (currentArguments.length == 1) {
            return targetArguments[0] > currentArguments[0];
        }
        return currentArguments.length == 2
                && widerDecimal(currentArguments[0], currentArguments[1],
                                targetArguments[0], targetArguments[1]);
    }

    private static long[] numericArguments(DatabaseType type) {
        List<String> values = type.arguments();
        long[] arguments = new long[values.size()];
        for (int index = 0; index < values.size(); index++) {
            arguments[index] = numericArgument(values.get(index));
        }
        return arguments;
    }

    private static long numericArgument(String value) {
        if ("max".equals(value)) {
            return Long.MAX_VALUE;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ignored) {
            return -1L;
        }
    }

    private static boolean widerDecimal(long currentPrecision,
                                         long currentScale,
                                         long targetPrecision,
                                         long targetScale) {
        long currentIntegerDigits = currentPrecision - currentScale;
        long targetIntegerDigits = targetPrecision - targetScale;
        return targetPrecision >= currentPrecision
                && targetScale >= currentScale
                && targetIntegerDigits >= currentIntegerDigits
                && (targetPrecision > currentPrecision || targetScale > currentScale);
    }

    private static String canonical(String value) {
        return DatabaseType.of(value).requireSafe("data type").canonical();
    }

    private static String baseType(DatabaseType type) {
        return switch (type.baseName()) {
            case "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE",
                    "TIMESTAMP WITHOUT TIME ZONE" -> "timestamp";
            case "TIME WITH TIME ZONE", "TIME WITH LOCAL TIME ZONE", "TIME WITHOUT TIME ZONE" -> "time";
            default -> type.baseName().toLowerCase(Locale.ROOT);
        };
    }
}
