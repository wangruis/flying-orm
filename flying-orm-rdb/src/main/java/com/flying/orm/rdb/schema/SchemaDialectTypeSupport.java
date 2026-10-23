package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.sql.render.SqlIdentifiers;
import com.flying.orm.core.type.DatabaseType;

import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.HexFormat;
import java.nio.charset.StandardCharsets;

/**
 * Small DDL type facade. Identifier quoting, type mapping and physical comparison have separate owners.
 */
final class SchemaDialectTypeSupport {

    private final String quoteOpen;
    private final String quoteClose;
    private final SchemaTypeMapping mapping;
    private final SchemaTypeComparison comparison;
    private final SchemaDialect.GeneratedValueStyle databaseStyle;

    SchemaDialectTypeSupport(String quoteOpen,
                             String quoteClose,
                             Map<String, DatabaseType> typeMappings,
                             Map<String, Function<DatabaseType, String>> typeRenderers,
                             SchemaDialect.GeneratedValueStyle databaseStyle) {
        this.quoteOpen = quoteOpen;
        this.quoteClose = quoteClose;
        SchemaDialect.GeneratedValueStyle style = Objects.requireNonNull(
                databaseStyle, "database style must not be null");
        this.databaseStyle = style;
        this.mapping = new SchemaTypeMapping(typeMappings, typeRenderers, style);
        this.comparison = new SchemaTypeComparison(style);
    }

    String identifier(String value) {
        String text = SqlIdentifiers.requireIdentifier(value);
        if (quoteOpen == null) {
            return text;
        }
        int dot = text.indexOf('.');
        if (dot < 0) {
            return quoteOpen + text + quoteClose;
        }
        return appendQuotedIdentifier(new StringBuilder(text.length()), text).toString();
    }

    StringBuilder appendIdentifier(StringBuilder sql, String value) {
        String text = SqlIdentifiers.requireIdentifier(value);
        return quoteOpen == null ? sql.append(text) : appendQuotedIdentifier(sql, text);
    }

    private StringBuilder appendQuotedIdentifier(StringBuilder sql, String text) {
        sql.append(quoteOpen);
        int start = 0;
        for (int dot = text.indexOf('.'); dot >= 0; dot = text.indexOf('.', start)) {
            sql.append(text, start, dot).append(quoteClose).append('.').append(quoteOpen);
            start = dot + 1;
        }
        return sql.append(text, start, text.length()).append(quoteClose);
    }

    String identifier(RelationIdentity value) {
        return namespaceObjectIdentifier(value, value.table());
    }

    String namespaceObjectIdentifier(RelationIdentity value, String objectName) {
        RelationIdentity relation = Objects.requireNonNull(value, "relation identity must not be null");
        if (relation.catalog().isEmpty() && relation.schema().isEmpty()) return identifierSegment(objectName);
        StringJoiner qualified = new StringJoiner(".");
        if (relation.catalog().isPresent()) {
            qualified.add(identifierSegment(relation.catalog().orElseThrow()));
        }
        if (relation.schema().isPresent()) {
            qualified.add(identifierSegment(relation.schema().orElseThrow()));
        }
        qualified.add(identifierSegment(objectName));
        return qualified.toString();
    }

    String schemaObjectIdentifier(RelationIdentity value, String objectName) {
        RelationIdentity relation = Objects.requireNonNull(value, "relation identity must not be null");
        if (relation.schema().isEmpty()) return identifierSegment(objectName);
        StringJoiner qualified = new StringJoiner(".");
        if (relation.schema().isPresent()) {
            qualified.add(identifierSegment(relation.schema().orElseThrow()));
        }
        qualified.add(identifierSegment(objectName));
        return qualified.toString();
    }

    private String identifierSegment(String value) {
        String text = SqlIdentifiers.requireIdentifier(value);
        return quoteOpen == null ? text : quoteOpen + text + quoteClose;
    }

    String dataType(String value) {
        return mapping.render(value);
    }

    String dataType(DatabaseType value, Integer length, Integer precision, Integer scale) {
        return mapping.render(value, length, precision, scale);
    }

    String physicalDataType(String value, Integer length, Integer precision, Integer scale) {
        return mapping.renderPhysical(value, length, precision, scale);
    }

    String metadataDataType(String value, Integer length, Integer precision, Integer scale) {
        return mapping.renderMetadata(value, length, precision, scale);
    }

    boolean sameDataType(String left, String right) {
        return comparison.same(left, right);
    }

    String quoteLiteral(String value) {
        String safeValue = Objects.requireNonNull(value, "literal must not be null");
        if (databaseStyle == SchemaDialect.GeneratedValueStyle.POSTGRESQL) {
            return "E'" + safeValue.replace("\\", "\\\\").replace("'", "''") + "'";
        }
        return "'" + safeValue.replace("'", "''") + "'";
    }

    String valueLiteral(String value) {
        if (databaseStyle == SchemaDialect.GeneratedValueStyle.MYSQL && value.indexOf('\\') >= 0) {
            // Hex text has the same value with and without NO_BACKSLASH_ESCAPES.
            return "_utf8mb4 X'" + HexFormat.of().formatHex(value.getBytes(StandardCharsets.UTF_8)) + "'";
        }
        return (databaseStyle == SchemaDialect.GeneratedValueStyle.SQL_SERVER ? "N" : "")
                + quoteLiteral(value);
    }

    static boolean safeWideningDataType(String current, String target) {
        return SchemaTypeComparison.safeWidening(current, target);
    }

    static String sqlServerCollation(String value) {
        String name = SqlIdentifiers.requireIdentifier(value);
        if (name.indexOf('.') >= 0) {
            throw new IllegalArgumentException("collation name must be a plain identifier");
        }
        return name;
    }

    static boolean sqlServerCharacterType(String type) {
        return switch (DatabaseType.of(type).baseName()) {
            case "CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "TEXT", "NTEXT" -> true;
            default -> false;
        };
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

    static String canonicalDataType(String value) {
        return DatabaseType.of(value).requireSafe("data type").canonical();
    }
}
