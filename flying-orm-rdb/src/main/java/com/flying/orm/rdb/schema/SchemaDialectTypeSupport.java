package com.flying.orm.rdb.schema;

import com.flying.orm.core.sql.render.SqlIdentifiers;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 处理 DDL 中最容易被忽略的两类安全输入：标识符和字段类型。
 *
 * <p>这两类内容不能像普通字段值那样使用绑定参数，所以所有入口都先经过这里的白名单规则。
 * 类型映射也在构建时校验，构建完成后只读共享。</p>
 */
final class SchemaDialectTypeSupport {

    private static final String TYPE_ARGUMENTS =
            "\\s*\\(\\s*(?:\\d+|max)\\s*(?:,\\s*\\d+\\s*)?\\)";
    private static final String INTERVAL_PRECISION = "\\s*\\(\\s*\\d+\\s*\\)";
    private static final String SIMPLE_TYPE = "[a-z_][a-z0-9_]*(?:\\.[a-z_][a-z0-9_]*)?";
    private static final String MULTI_WORD_TYPE =
            "(?:double\\s+precision|character\\s+varying|national\\s+character\\s+varying|bit\\s+varying)";
    private static final String TIME_ZONE_TYPE =
            "time(?:stamp)?(?:" + TYPE_ARGUMENTS + ")?\\s+(?:with(?:\\s+local)?|without)\\s+time\\s+zone";
    private static final String INTERVAL_TYPE =
            "interval\\s+(?:year(?:" + INTERVAL_PRECISION + ")?\\s+to\\s+month"
                    + "|day(?:" + INTERVAL_PRECISION + ")?\\s+to\\s+second(?:" + INTERVAL_PRECISION + ")?)";
    private static final Pattern SAFE_DATA_TYPE = Pattern.compile(
            "(?i)(?:" + TIME_ZONE_TYPE
                    + "|" + INTERVAL_TYPE
                    + "|" + MULTI_WORD_TYPE + "(?:" + TYPE_ARGUMENTS + ")?"
                    + "|" + SIMPLE_TYPE + "(?:" + TYPE_ARGUMENTS + ")?"
                    + "(?:\\s+unsigned)?(?:\\s+zerofill)?)(?:\\[\\])*" );
    private static final Pattern TYPE_MODIFIER_SUFFIX = Pattern.compile(
            "(?i)\\s+(?:(?:with(?:\\s+local)?|without)\\s+time\\s+zone"
                    + "|(?:unsigned(?:\\s+zerofill)?|zerofill))(?:\\[\\])*\\s*$");
    private static final Pattern TYPE_WHITESPACE = Pattern.compile("\\s+");
    private static final Pattern TYPE_ARGUMENT_WHITESPACE = Pattern.compile("\\s*([(),])\\s*");

    private final String quoteOpen;
    private final String quoteClose;
    private final Map<String, String> typeMappings;

    SchemaDialectTypeSupport(String quoteOpen,
                             String quoteClose,
                             Map<String, String> typeMappings) {
        this.quoteOpen = quoteOpen;
        this.quoteClose = quoteClose;
        this.typeMappings = Map.copyOf(new LinkedHashMap<>(typeMappings));
    }

    String identifier(String value) {
        String text = SqlIdentifiers.requireIdentifier(value, "identifier");
        if (quoteOpen == null) {
            return text;
        }
        StringJoiner joiner = new StringJoiner(".");
        for (String part : text.split("\\.")) {
            joiner.add(quoteOpen + part + quoteClose);
        }
        return joiner.toString();
    }

    String dataType(String value) {
        String text = requireDataType(value, "data type");
        return requireDataType(typeMappings.getOrDefault(normalize(text), text), "mapped data type");
    }

    String dataType(String value, Integer length, Integer precision, Integer scale) {
        return withTypeArguments(dataType(value), length, precision, scale);
    }

    private static String withTypeArguments(String type, Integer length, Integer precision, Integer scale) {
        if (length != null) {
            return withArguments(type, String.valueOf(length));
        }
        if (precision != null && scale != null) {
            return withArguments(type, precision + "," + scale);
        }
        if (precision != null) {
            return withArguments(type, String.valueOf(precision));
        }
        return type;
    }

    String quoteLiteral(String value) {
        return "'" + requireText(value, "literal").replace("'", "''") + "'";
    }

    static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }

    static String requireDataType(String value, String fieldName) {
        String text = requireText(value, fieldName);
        if (!SAFE_DATA_TYPE.matcher(text).matches()) {
            throw new IllegalArgumentException(fieldName + " contains unsupported SQL type syntax");
        }
        return text;
    }

    static String normalize(String value) {
        return value.trim().toLowerCase(Locale.ROOT);
    }

    /**
     * 把已经通过白名单的类型文本规范为可比较形式。这里只折叠大小写和无语义空白，不改写类型名称、参数或修饰符。
     */
    static String canonicalDataType(String value) {
        String normalized = TYPE_WHITESPACE.matcher(requireDataType(value, "data type").toLowerCase(Locale.ROOT))
                                           .replaceAll(" ");
        return TYPE_ARGUMENT_WHITESPACE.matcher(normalized).replaceAll("$1");
    }

    static boolean sameDataType(String left, String right) {
        return canonicalDataType(left).equals(canonicalDataType(right));
    }

    /** 只把同一物理类型的长度或精度扩张判为安全，跨类型和无法解析的参数一律交给审核路径。 */
    static boolean safeWideningDataType(String current, String target) {
        String currentType = canonicalDataType(current);
        String targetType = canonicalDataType(target);
        if (currentType.equals(targetType) || !baseDataType(currentType).equals(baseDataType(targetType))) {
            return false;
        }
        long[] currentArguments = typeArguments(currentType);
        long[] targetArguments = typeArguments(targetType);
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

    /** 去掉首组数值参数后保留数组、时区和 unsigned 等修饰符，跨基础类型时不猜测安全转换。 */
    private static String baseDataType(String value) {
        int open = value.indexOf('(');
        if (open < 0) {
            return value;
        }
        int close = value.indexOf(')', open + 1);
        return close < 0 ? value : value.substring(0, open) + value.substring(close + 1);
    }

    /** 只解析白名单允许的首组长度或精度参数；复杂 interval 变化会因基础类型不同而保守拒绝。 */
    private static long[] typeArguments(String value) {
        int open = value.indexOf('(');
        int close = open < 0 ? -1 : value.indexOf(')', open + 1);
        if (open < 0 || close < 0) {
            return new long[0];
        }
        String[] values = value.substring(open + 1, close).split(",", -1);
        long[] arguments = new long[values.length];
        for (int index = 0; index < values.length; index++) {
            arguments[index] = typeArgument(values[index]);
        }
        return arguments;
    }

    /** 超出 long 的数据库专用参数不自动判成安全扩容，仍交给显式审核路径。 */
    private static long typeArgument(String value) {
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

    private static String withArguments(String type, String arguments) {
        int open = type.indexOf('(');
        if (open < 0) {
            Matcher modifier = TYPE_MODIFIER_SUFFIX.matcher(type);
            int arrayStart = type.indexOf('[');
            int insertAt = modifier.find() ? modifier.start() : arrayStart >= 0 ? arrayStart : type.length();
            return type.substring(0, insertAt) + "(" + arguments + ")" + type.substring(insertAt);
        }
        int close = type.lastIndexOf(')');
        return close < open ? type : type.substring(0, open + 1) + arguments + type.substring(close);
    }
}
