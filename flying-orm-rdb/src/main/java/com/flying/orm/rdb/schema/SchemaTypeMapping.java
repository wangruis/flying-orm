package com.flying.orm.rdb.schema;

import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.core.type.LogicalType;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
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
            "boolean", "bool", "date", "xml", "sqlxml", "enum", "set");
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

    private final Map<String, DatabaseType> mappings;
    private final Map<String, Function<DatabaseType, String>> renderers;
    private final SchemaDialect.GeneratedValueStyle databaseStyle;

    SchemaTypeMapping(Map<String, DatabaseType> mappings,
                      Map<String, Function<DatabaseType, String>> renderers,
                      SchemaDialect.GeneratedValueStyle databaseStyle) {
        this.mappings = Map.copyOf(Objects.requireNonNull(mappings, "type mappings must not be null"));
        this.renderers = Map.copyOf(renderers);
        this.databaseStyle = Objects.requireNonNull(databaseStyle, "database style must not be null");
    }

    String render(String value) {
        DatabaseType logical = safeType(value, "data type");
        String custom = customType(logical, null, null, null);
        if (custom != null) return custom;
        DatabaseType mapped = mappedType(logical);
        validateTemporalPrecision(logical, mapped, null);
        return mapped.declaration();
    }

    String render(DatabaseType logical, Integer length, Integer precision, Integer scale) {
        logical.requireSafe("data type");
        String custom = customType(logical, length, precision, scale);
        if (custom != null) return custom;
        return mappedType(logical, length, precision, scale);
    }

    /** Legacy metadata retains built-in logical aliases, but has already applied custom type sizing. */
    String renderMetadata(String value, Integer length, Integer precision, Integer scale) {
        return mappedType(safeType(value, "metadata data type"), length, precision, scale);
    }

    private String mappedType(DatabaseType logical, Integer length, Integer precision, Integer scale) {
        validateTemporalArguments(logical, length, scale);
        DatabaseType physical = mappedType(logical);
        validateTemporalPrecision(logical, physical, precision);
        if (isTextBackedTemporalType(logical, physical)) {
            return withTypeArguments(physical, length, null, null);
        }
        return withTypeArguments(physical, length, precision, scale);
    }

    String renderPhysical(String value, Integer length, Integer precision, Integer scale) {
        DatabaseType physical = safeType(value, "physical data type");
        if (!physical.arguments().isEmpty()) {
            return physical.declaration();
        }
        return withTypeArguments(physical, length, precision, scale);
    }

    private String customType(DatabaseType logical, Integer length, Integer precision, Integer scale) {
        if (renderers.isEmpty()) return null;
        DatabaseType input = length == null && precision == null && scale == null ? logical
                : safeType(applyTypeArguments(logical, length, precision, scale), "data type");
        Function<DatabaseType, String> renderer = renderers.get(input.canonical());
        if (renderer == null && !mappings.containsKey(logical.canonical())) {
            renderer = renderers.get(input.comparisonShape());
        }
        if (renderer == null) return null;
        return safeType(renderer.apply(input), "rendered data type").declaration();
    }

    private DatabaseType mappedType(DatabaseType type) {
        if (databaseStyle == SchemaDialect.GeneratedValueStyle.POSTGRESQL && type.isArray()) {
            String scalar = type.canonical().substring(
                    0, type.canonical().length() - type.arrayDimensions() * 2);
            return safeType(mappedType(safeType(scalar, "array element data type")).declaration()
                    + "[]".repeat(type.arrayDimensions()), "mapped data type");
        }
        DatabaseType direct = mappings.get(type.canonical());
        if (direct != null) {
            return mappedTemporalCapacity(type, direct);
        }
        String scalar = mappedScalarModifierType(type);
        if (scalar != null) {
            return safeType(scalar, "mapped data type");
        }
        DatabaseType temporal = mappedTemporalType(type);
        if (temporal != null) {
            return mappedTemporalCapacity(type, temporal);
        }
        if (type.logicalType() == LogicalType.UUID
                && "UUID".equals(type.baseName())
                && !mappings.containsKey(type.canonical())) {
            throw new IllegalArgumentException("UUID requires an explicit schema dialect type mapping");
        }
        return type;
    }

    private String mappedScalarModifierType(DatabaseType type) {
        String normalized = type.canonical();
        for (String suffix : SCALAR_MODIFIER_SUFFIXES) {
            if (normalized.endsWith(suffix)) {
                String base = normalized.substring(0, normalized.length() - suffix.length()).stripTrailing();
                DatabaseType mappedBase = mappings.get(base);
                return mappedBase == null ? null : mappedBase.declaration() + suffix.toUpperCase(Locale.ROOT);
            }
        }
        return null;
    }

    private DatabaseType mappedTemporalType(DatabaseType type) {
        String logicalType = switch (type.baseName()) {
            case "TIMESTAMP WITH TIME ZONE" -> "timestamptz";
            case "TIMESTAMP WITHOUT TIME ZONE" -> "timestamp";
            case "TIME WITH TIME ZONE" -> "offset_time";
            case "TIME WITHOUT TIME ZONE" -> "time";
            case "TIMESTAMP WITH LOCAL TIME ZONE" -> localTimestampType();
            case "TIME WITH LOCAL TIME ZONE" -> throw new IllegalArgumentException(
                    "time with local time zone is not a supported SQL data type");
            default -> {
                String base = baseType(type);
                yield TEMPORAL_TYPES.contains(base) ? base : null;
            }
        };
        if (logicalType == null) {
            return null;
        }
        DatabaseType mappedBase = "timestamp with local time zone".equals(logicalType)
                ? safeType("TIMESTAMP WITH LOCAL TIME ZONE", "mapped data type")
                : mappings.get(logicalType);
        if (mappedBase == null) {
            return null;
        }
        boolean preserveBase = type.arguments().isEmpty() || isTextBackedTemporalType(type, mappedBase);
        if (preserveBase && !type.isArray()) return mappedBase;
        String mapped = preserveBase ? mappedBase.declaration()
                : withArguments(mappedBase, String.join(",", type.arguments()));
        return safeType(
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
    private DatabaseType mappedTemporalCapacity(DatabaseType logicalType, DatabaseType safeMapped) {
        String logicalBase = baseType(logicalType);
        if (databaseStyle != SchemaDialect.GeneratedValueStyle.ORACLE
                || !("time".equals(logicalBase) || "time without time zone".equals(logicalBase))
                || !CHARACTER_TYPES.contains(baseType(safeMapped))) {
            return safeMapped;
        }
        long[] arguments = numericArguments(safeMapped);
        return arguments.length == 1 && arguments[0] < ISO_LOCAL_TIME_MAX_LENGTH
                ? safeType(withArguments(safeMapped, String.valueOf(ISO_LOCAL_TIME_MAX_LENGTH)),
                        "mapped data type")
                : safeMapped;
    }

    private String withTypeArguments(DatabaseType type, Integer length, Integer precision, Integer scale) {
        if ((length != null || precision != null || scale != null) && !acceptsTypeArguments(type)) {
            throw new IllegalArgumentException("data type does not accept length or precision arguments: " + type);
        }
        return applyTypeArguments(type, length, precision, scale);
    }

    private static String applyTypeArguments(DatabaseType type, Integer length, Integer precision, Integer scale) {
        if (length != null) {
            return withArguments(type, String.valueOf(length));
        }
        if (precision != null && scale != null) {
            return withArguments(type, precision + "," + scale);
        }
        return precision == null ? type.declaration() : withArguments(type, String.valueOf(precision));
    }

    private boolean acceptsTypeArguments(DatabaseType type) {
        String base = baseType(type);
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
        String base = type.arguments().isEmpty() && !type.isArray() && !type.unsigned() && !type.zerofill()
                ? type.canonical() : type.baseName().toLowerCase(Locale.ROOT);
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
            String value = values.get(index);
            if (!characterLengthUnit(type).isEmpty()) {
                value = value.substring(0, value.indexOf(' '));
            }
            try {
                arguments[index] = Long.parseLong(value);
            } catch (NumberFormatException ignored) {
                arguments[index] = -1L;
            }
        }
        return arguments;
    }

    private static DatabaseType safeType(String value, String name) {
        return DatabaseType.of(value).requireSafe(name);
    }

    private static String withArguments(DatabaseType parsedType, String arguments) {
        String type = parsedType.declaration();
        String unit = characterLengthUnit(parsedType);
        if (!unit.isEmpty() && arguments.chars().allMatch(Character::isDigit)) {
            arguments += " " + unit.toUpperCase(Locale.ROOT);
        }
        int open = type.indexOf('(');
        if (open >= 0) {
            int close = type.lastIndexOf(')');
            return close < open ? type : type.substring(0, open + 1) + arguments + type.substring(close);
        }
        int arrayStart = type.indexOf('[');
        int insertAt = arrayStart >= 0 ? arrayStart : type.length();
        if (parsedType.unsigned() || parsedType.zerofill() || parsedType.baseName().endsWith("TIME ZONE")) {
            Matcher modifier = TYPE_MODIFIER_SUFFIX.matcher(type);
            if (modifier.find()) insertAt = modifier.start();
        }
        return type.substring(0, insertAt) + "(" + arguments + ")" + type.substring(insertAt);
    }

    /** 显式声明优先；已有 Oracle 字符列未指定新单位时保留物理单位，不猜测会话默认值。 */
    static String preserveCharacterLengthUnit(SchemaDialect dialect, String actual, String desired) {
        if (dialect.generatedValueStyle() != SchemaDialect.GeneratedValueStyle.ORACLE) return desired;
        DatabaseType target = DatabaseType.of(desired);
        String unit = characterLengthUnit(DatabaseType.of(actual));
        if (unit.isEmpty() || !characterLengthUnit(target).isEmpty()
                || !("CHAR".equals(target.baseName()) || "VARCHAR2".equals(target.baseName()))
                || target.arguments().size() != 1) return desired;
        return withArguments(target, target.arguments().getFirst() + " " + unit.toUpperCase(Locale.ROOT));
    }

    static String characterLengthUnit(DatabaseType type) {
        if (!type.safeDeclaration() || type.isArray() || type.arguments().size() != 1
                || !("CHAR".equals(type.baseName()) || "VARCHAR2".equals(type.baseName()))) return "";
        String argument = type.arguments().getFirst();
        int separator = argument.indexOf(' ');
        return separator < 0 ? "" : argument.substring(separator + 1);
    }
}
