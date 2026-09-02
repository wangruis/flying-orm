package com.flying.orm.rdb.schema;

import com.flying.orm.core.type.DatabaseType;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Maps a validated logical database type to one dialect's DDL declaration. */
final class SchemaTypeMapping {

    private static final List<String> SCALAR_MODIFIER_SUFFIXES =
            List.of(" unsigned zerofill", " unsigned", " zerofill");
    private static final Pattern TYPE_MODIFIER_SUFFIX = Pattern.compile(
            "(?i)\\s+(?:(?:with(?:\\s+local)?|without)\\s+time\\s+zone"
                    + "|(?:unsigned(?:\\s+zerofill)?|zerofill))(?:\\[\\])*\\s*$");
    private static final Set<String> TYPES_WITHOUT_ARGUMENTS = Set.of(
            "blob", "clob", "nclob", "text", "tinytext", "mediumtext", "longtext",
            "tinyblob", "mediumblob", "longblob", "bytea", "json", "jsonb", "uuid",
            "boolean", "bool", "date", "xml", "sqlxml");
    private static final Set<String> POSTGRESQL_INTEGER_TYPES = Set.of(
            "smallint", "int2", "integer", "int", "int4", "bigint", "int8",
            "smallserial", "serial2", "serial", "serial4", "bigserial", "serial8",
            "real", "float4", "double precision", "float8", "money");
    private static final Set<String> SQL_SERVER_FIXED_TYPES = Set.of(
            "tinyint", "smallint", "int", "integer", "bigint", "bit", "real", "money", "smallmoney",
            "datetime", "smalldatetime", "image", "ntext", "uniqueidentifier", "rowversion", "timestamp");
    private static final Set<String> TEMPORAL_TYPES = Set.of(
            "time", "offset_time", "timetz", "timestamp", "timestamptz", "datetime", "datetime2",
            "datetimeoffset");
    private static final Set<String> FIXED_TEMPORAL_TYPES = Set.of(
            "date", "oracle_date", "smalldatetime", "sqlserver_datetime", "sqlserver_smalldatetime");
    private static final Set<String> CHARACTER_TYPES = Set.of(
            "char", "nchar", "varchar", "varchar2", "nvarchar", "nvarchar2",
            "character", "character varying", "national character varying");
    private static final Set<String> H2_TEMPORAL_PRECISION_TYPES = Set.of("time", "timestamp");
    private static final Set<String> POSTGRESQL_TEMPORAL_PRECISION_TYPES = Set.of(
            "time", "timetz", "timestamp", "timestamptz");
    private static final Set<String> MYSQL_TEMPORAL_PRECISION_TYPES = Set.of(
            "time", "datetime", "timestamp");
    private static final Set<String> SQL_SERVER_TEMPORAL_PRECISION_TYPES = Set.of(
            "time", "datetime2", "datetimeoffset");
    private static final long ISO_LOCAL_TIME_MAX_LENGTH = 18L;

    private final Map<String, String> mappings;
    private final SchemaDialect.GeneratedValueStyle databaseStyle;

    SchemaTypeMapping(Map<String, String> mappings, SchemaDialect.GeneratedValueStyle databaseStyle) {
        this.mappings = Map.copyOf(Objects.requireNonNull(mappings, "type mappings must not be null"));
        this.databaseStyle = Objects.requireNonNull(databaseStyle, "database style must not be null");
    }

    String render(String value) {
        DatabaseType logical = safeType(value, "data type");
        String mapped = mappedType(logical);
        validateTemporalPrecision(logical, safeType(mapped, "mapped data type"), null);
        return mapped;
    }

    String render(String value, Integer length, Integer precision, Integer scale) {
        DatabaseType logical = safeType(value, "data type");
        validateTemporalArguments(logical, length, scale);
        String mapped = mappedType(logical);
        DatabaseType physical = safeType(mapped, "mapped data type");
        validateTemporalPrecision(logical, physical, precision);
        if (isTextBackedTemporalType(logical, physical)) {
            return withTypeArguments(mapped, length, null, null);
        }
        return withTypeArguments(mapped, length, precision, scale);
    }

    private String mappedType(DatabaseType type) {
        String direct = mappings.get(type.canonical());
        if (direct != null) {
            return mappedTemporalCapacity(type, direct);
        }
        String scalar = mappedScalarModifierType(type);
        if (scalar != null) {
            return scalar;
        }
        String temporal = mappedTemporalType(type);
        return temporal == null ? type.declaration() : mappedTemporalCapacity(type, temporal);
    }

    private String mappedScalarModifierType(DatabaseType type) {
        String normalized = type.canonical();
        for (String suffix : SCALAR_MODIFIER_SUFFIXES) {
            if (normalized.endsWith(suffix)) {
                String base = normalized.substring(0, normalized.length() - suffix.length()).stripTrailing();
                String mappedBase = mappings.get(base);
                return mappedBase == null ? null : mappedBase + suffix.toUpperCase(Locale.ROOT);
            }
        }
        return null;
    }

    private String mappedTemporalType(DatabaseType type) {
        String logicalType = switch (type.baseName()) {
            case "TIMESTAMP WITH TIME ZONE" -> "timestamptz";
            case "TIMESTAMP WITHOUT TIME ZONE" -> "timestamp";
            case "TIME WITH TIME ZONE" -> "offset_time";
            case "TIME WITHOUT TIME ZONE" -> "time";
            case "TIMESTAMP WITH LOCAL TIME ZONE" -> localTimestampType();
            case "TIME WITH LOCAL TIME ZONE" -> throw new IllegalArgumentException(
                    "time with local time zone is not a supported SQL data type");
            default -> {
                String base = type.baseName().toLowerCase(Locale.ROOT);
                yield TEMPORAL_TYPES.contains(base) ? base : null;
            }
        };
        if (logicalType == null) {
            return null;
        }
        String mappedBase = "timestamp with local time zone".equals(logicalType)
                ? "TIMESTAMP WITH LOCAL TIME ZONE"
                : mappings.get(logicalType);
        if (mappedBase == null) {
            return null;
        }
        String mapped = type.arguments().isEmpty()
                || isTextBackedTemporalType(type, safeType(mappedBase, "mapped data type"))
                ? mappedBase
                : withArguments(mappedBase, String.join(",", type.arguments()));
        return SchemaDialectTypeSupport.requireDataType(
                mapped + "[]".repeat(type.arrayDimensions()), "mapped data type");
    }

    private String localTimestampType() {
        if (databaseStyle != SchemaDialect.GeneratedValueStyle.ORACLE) {
            throw new IllegalArgumentException(
                    "timestamp with local time zone is only supported by the Oracle schema dialect");
        }
        return "timestamp with local time zone";
    }

    /** Oracle stores local time in text; 18 characters are required for nanosecond ISO values. */
    private String mappedTemporalCapacity(DatabaseType logicalType, String mappedType) {
        DatabaseType safeMapped = safeType(mappedType, "mapped data type");
        String logicalBase = baseType(logicalType);
        if (databaseStyle != SchemaDialect.GeneratedValueStyle.ORACLE
                || !("time".equals(logicalBase) || "time without time zone".equals(logicalBase))
                || !CHARACTER_TYPES.contains(baseType(safeMapped))) {
            return safeMapped.declaration();
        }
        long[] arguments = numericArguments(safeMapped);
        return arguments.length == 1 && arguments[0] < ISO_LOCAL_TIME_MAX_LENGTH
                ? withArguments(safeMapped.declaration(), String.valueOf(ISO_LOCAL_TIME_MAX_LENGTH))
                : safeMapped.declaration();
    }

    private String withTypeArguments(String type, Integer length, Integer precision, Integer scale) {
        if ((length != null || precision != null || scale != null) && !acceptsTypeArguments(type)) {
            throw new IllegalArgumentException("data type does not accept length or precision arguments: " + type);
        }
        if (length != null) {
            return withArguments(type, String.valueOf(length));
        }
        if (precision != null && scale != null) {
            return withArguments(type, precision + "," + scale);
        }
        return precision == null ? type : withArguments(type, String.valueOf(precision));
    }

    private boolean acceptsTypeArguments(String type) {
        String base = baseType(safeType(type, "data type"));
        if (base.startsWith("interval ")) {
            return false;
        }
        int dot = base.lastIndexOf('.');
        base = dot < 0 ? base : base.substring(dot + 1);
        if (TYPES_WITHOUT_ARGUMENTS.contains(base)) {
            return false;
        }
        return switch (databaseStyle) {
            case POSTGRESQL -> !POSTGRESQL_INTEGER_TYPES.contains(base);
            case SQL_SERVER -> !SQL_SERVER_FIXED_TYPES.contains(base);
            default -> true;
        };
    }

    private static void validateTemporalArguments(DatabaseType logicalType, Integer length, Integer scale) {
        String base = baseType(logicalType);
        if (!TEMPORAL_TYPES.contains(base) && !FIXED_TEMPORAL_TYPES.contains(base)) {
            return;
        }
        if (length != null) {
            throw new IllegalArgumentException("temporal data type must not define length");
        }
        if (scale != null) {
            throw new IllegalArgumentException("temporal data type must not define numeric scale");
        }
    }

    private void validateTemporalPrecision(DatabaseType logicalType, DatabaseType mappedType, Integer precision) {
        String base = baseType(logicalType);
        if (!TEMPORAL_TYPES.contains(base) && !FIXED_TEMPORAL_TYPES.contains(base)) {
            return;
        }
        Integer inline = inlineTemporalPrecision(logicalType);
        if (FIXED_TEMPORAL_TYPES.contains(base)) {
            if (precision != null || inline != null) {
                throw new IllegalArgumentException("fixed temporal data type must not define precision");
            }
            return;
        }
        if (precision != null && inline != null && !precision.equals(inline)) {
            throw new IllegalArgumentException("conflicting temporal precision values");
        }
        Integer effective = precision == null ? inline : precision;
        if (effective == null) {
            return;
        }
        if (effective < 0) {
            throw new IllegalArgumentException("temporal precision must not be negative");
        }
        int maximum = isTextBackedTemporalType(logicalType, mappedType)
                ? 9 : nativeTemporalPrecisionMaximum(baseType(mappedType));
        if (maximum >= 0 && effective > maximum) {
            throw new IllegalArgumentException(
                    "temporal precision exceeds " + databaseStyle + " maximum " + maximum);
        }
    }

    private int nativeTemporalPrecisionMaximum(String physicalType) {
        return switch (databaseStyle) {
            case H2 -> H2_TEMPORAL_PRECISION_TYPES.contains(physicalType) ? 9 : -1;
            case MYSQL -> MYSQL_TEMPORAL_PRECISION_TYPES.contains(physicalType) ? 6 : -1;
            case POSTGRESQL -> POSTGRESQL_TEMPORAL_PRECISION_TYPES.contains(physicalType) ? 6 : -1;
            case ORACLE -> "timestamp".equals(physicalType) ? 9 : -1;
            case SQL_SERVER -> SQL_SERVER_TEMPORAL_PRECISION_TYPES.contains(physicalType) ? 7 : -1;
            case NONE -> -1;
        };
    }

    private static Integer inlineTemporalPrecision(DatabaseType logicalType) {
        long[] arguments = numericArguments(logicalType);
        if (arguments.length == 0) {
            return null;
        }
        if (arguments.length != 1 || arguments[0] < 0 || arguments[0] > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("invalid temporal precision");
        }
        return (int) arguments[0];
    }

    private static boolean isTextBackedTemporalType(DatabaseType logicalType, DatabaseType mappedType) {
        return TEMPORAL_TYPES.contains(baseType(logicalType))
                && CHARACTER_TYPES.contains(baseType(mappedType));
    }

    private static String baseType(DatabaseType type) {
        String base = type.baseName().toLowerCase(Locale.ROOT);
        return switch (base) {
            case "timestamp with time zone", "timestamp with local time zone",
                    "timestamp without time zone" -> "timestamp";
            case "time with time zone", "time with local time zone", "time without time zone" -> "time";
            default -> base;
        };
    }

    private static long[] numericArguments(DatabaseType type) {
        List<String> values = type.arguments();
        long[] arguments = new long[values.size()];
        for (int index = 0; index < values.size(); index++) {
            try {
                arguments[index] = Long.parseLong(values.get(index));
            } catch (NumberFormatException ignored) {
                arguments[index] = -1L;
            }
        }
        return arguments;
    }

    private static DatabaseType safeType(String value, String name) {
        return DatabaseType.of(value).requireSafe(name);
    }

    private static String withArguments(String type, String arguments) {
        int open = type.indexOf('(');
        if (open >= 0) {
            int close = type.lastIndexOf(')');
            return close < open ? type : type.substring(0, open + 1) + arguments + type.substring(close);
        }
        Matcher modifier = TYPE_MODIFIER_SUFFIX.matcher(type);
        int arrayStart = type.indexOf('[');
        int insertAt = modifier.find() ? modifier.start() : arrayStart >= 0 ? arrayStart : type.length();
        return type.substring(0, insertAt) + "(" + arguments + ")" + type.substring(insertAt);
    }
}
