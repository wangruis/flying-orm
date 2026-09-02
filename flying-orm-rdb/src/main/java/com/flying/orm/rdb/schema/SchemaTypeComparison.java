package com.flying.orm.rdb.schema;

import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.core.type.LogicalType;

import java.util.List;
import java.util.Set;

/** Compares physical DDL type shapes without changing their runtime meaning. */
final class SchemaTypeComparison {

    private static final Set<String> MYSQL_INTEGER_TYPES =
            Set.of("TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT");
    private static final Set<String> POSTGRESQL_DEFAULT_PRECISION_TYPES = Set.of(
            "time", "timetz", "timestamp", "timestamptz");
    private static final Set<String> SQL_SERVER_DEFAULT_PRECISION_TYPES = Set.of(
            "time", "datetime2", "datetimeoffset");

    private final SchemaDialect.GeneratedValueStyle databaseStyle;

    SchemaTypeComparison(SchemaDialect.GeneratedValueStyle databaseStyle) {
        this.databaseStyle = databaseStyle;
    }

    boolean same(String left, String right) {
        return comparable(left).equals(comparable(right));
    }

    private String comparable(String value) {
        String type = comparableOracleNumber(canonical(value));
        type = comparableH2CharacterType(type);
        type = comparableMysqlBit(type);
        type = comparableMysqlInteger(type);
        DatabaseType parsed = DatabaseType.of(type).requireSafe("data type");
        if (parsed.arguments().size() != 1
                || !isDefaultTemporalPrecision(baseType(parsed), parsed.arguments().getFirst())) {
            return type;
        }
        return parsed.comparisonShape();
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

    private String comparableH2CharacterType(String type) {
        if (databaseStyle != SchemaDialect.GeneratedValueStyle.H2) {
            return type;
        }
        DatabaseType parsed = DatabaseType.of(type).requireSafe("data type");
        if (parsed.arguments().isEmpty()
                && ("TEXT".equals(parsed.baseName())
                    || "VARCHAR".equals(parsed.baseName())
                    || "CHARACTER VARYING".equals(parsed.baseName()))) {
            return "character varying(1000000000)";
        }
        return "VARCHAR".equals(parsed.baseName())
                ? "character varying" + type.substring("varchar".length()) : type;
    }

    private String comparableOracleNumber(String type) {
        if (databaseStyle != SchemaDialect.GeneratedValueStyle.ORACLE) {
            return type;
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
            default -> type.baseName().toLowerCase(java.util.Locale.ROOT);
        };
    }
}
