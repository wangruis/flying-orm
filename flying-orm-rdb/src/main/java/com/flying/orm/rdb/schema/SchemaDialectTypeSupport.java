package com.flying.orm.rdb.schema;

import com.flying.orm.core.sql.render.SqlIdentifiers;
import com.flying.orm.core.type.DatabaseType;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Small DDL type facade. Identifier quoting, type mapping and physical comparison have separate owners.
 */
final class SchemaDialectTypeSupport {

    private final String quoteOpen;
    private final String quoteClose;
    private final SchemaTypeMapping mapping;
    private final SchemaTypeComparison comparison;

    SchemaDialectTypeSupport(String quoteOpen,
                             String quoteClose,
                             Map<String, String> typeMappings,
                             SchemaDialect.GeneratedValueStyle databaseStyle) {
        this.quoteOpen = quoteOpen;
        this.quoteClose = quoteClose;
        Map<String, String> mappings = Map.copyOf(new LinkedHashMap<>(typeMappings));
        SchemaDialect.GeneratedValueStyle style = Objects.requireNonNull(
                databaseStyle, "database style must not be null");
        this.mapping = new SchemaTypeMapping(mappings, style);
        this.comparison = new SchemaTypeComparison(style);
    }

    String identifier(String value) {
        String text = SqlIdentifiers.requireIdentifier(value, "identifier");
        if (quoteOpen == null) {
            return text;
        }
        StringJoiner quoted = new StringJoiner(".");
        for (String part : text.split("\\.")) {
            quoted.add(quoteOpen + part + quoteClose);
        }
        return quoted.toString();
    }

    String dataType(String value) {
        return mapping.render(value);
    }

    String dataType(String value, Integer length, Integer precision, Integer scale) {
        return mapping.render(value, length, precision, scale);
    }

    boolean sameDataType(String left, String right) {
        return comparison.same(left, right);
    }

    String quoteLiteral(String value) {
        return "'" + requireText(value, "literal").replace("'", "''") + "'";
    }

    static boolean safeWideningDataType(String current, String target) {
        return SchemaTypeComparison.safeWidening(current, target);
    }

    static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }

    static String requireDataType(String value, String fieldName) {
        return DatabaseType.of(value).requireSafe(fieldName).declaration();
    }

    static String normalize(String value) {
        return value.trim().toLowerCase(Locale.ROOT);
    }

    static String canonicalDataType(String value) {
        return DatabaseType.of(value).requireSafe("data type").canonical();
    }
}
