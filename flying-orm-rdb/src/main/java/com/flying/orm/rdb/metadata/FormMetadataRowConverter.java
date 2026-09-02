package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlIdentifiers;
import com.flying.orm.core.type.DatabaseType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 把字典查询结果转换成 flying-orm 的元数据对象。
 *
 * <p>JDBC 和 R2DBC 的差别只在“怎么把 SQL 跑起来”。驱动返回的列名大小写、布尔值和数字类型
 * 可能略有不同，所以这些兼容规则必须集中在这里，不能让两个执行路径各写一份。</p>
 */
final class FormMetadataRowConverter {

    private static final Pattern POSTGRES_SEQUENCE_DEFAULT = Pattern.compile(
            "(?i)^nextval\\(\\s*'((?:''|[^'])+)'\\s*::\\s*regclass\\s*\\)$");
    private static final Pattern NEXT_VALUE_SEQUENCE_DEFAULT = Pattern.compile(
            "(?i)^next\\s+value\\s+for\\s+(.+)$");
    private static final Pattern ORACLE_SEQUENCE_DEFAULT = Pattern.compile(
            "(?i)^(.+?)\\.nextval$");

    private FormMetadataRowConverter() {
    }

    static DynamicForm toDynamicForm(String formId,
                                     String table,
                                     List<? extends Map<String, Object>> rows,
                                     Function<String, String> typeMapper) {
        Objects.requireNonNull(rows, "metadata rows must not be null");
        Function<String, String> safeTypeMapper = Objects.requireNonNull(typeMapper, "type mapper must not be null");
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("table metadata not found");
        }
        DynamicForm.Builder builder = DynamicForm.builder(formId, table);
        for (Map<String, Object> row : rows) {
            builder.addField(toField(row, safeTypeMapper));
        }
        return builder.build();
    }

    static TableMetadata toTableMetadata(String table,
                                         DynamicForm form,
                                         List<? extends Map<String, Object>> indexRows,
                                         List<? extends Map<String, Object>> foreignKeyRows) {
        Objects.requireNonNull(form, "dynamic form must not be null");
        TableMetadata.Builder builder = TableMetadata.builder(table);
        form.toTableMetadata().columns().forEach(builder::addColumn);
        toIndexes(indexRows).forEach(builder::addIndex);
        toForeignKeys(foreignKeyRows).forEach(builder::addForeignKey);
        return builder.build();
    }

    private static DynamicField toField(Map<String, Object> row, Function<String, String> typeMapper) {
        String name = text(row, "COLUMN_NAME");
        String dataType = typeMapper.apply(text(row, "DATA_TYPE"));
        DynamicField field = bool(row, "PRIMARY_KEY")
                ? DynamicField.primaryKey(name, dataType)
                : DynamicField.of(name, dataType).withNullable(nullable(row));
        field = applyTypeArguments(field, row);
        field = applyGeneration(field, row);
        String comment = optionalText(row, "REMARKS");
        return comment == null ? field : field.withComment(comment);
    }

    private static DynamicField applyGeneration(DynamicField field, Map<String, Object> row) {
        if (bool(row, "IS_IDENTITY")) {
            return field.withGeneration(ValueGeneration.identity());
        }
        String sequenceName = sequenceName(optionalText(row, "GENERATION_EXPRESSION"),
                                           optionalText(row, "RESOLUTION_SCHEMA"));
        return sequenceName == null
                ? field
                : field.withGeneration(ValueGeneration.sequence(sequenceName));
    }

    /** 只识别 flying-orm 会生成的三类 sequence 默认值；其他数据库表达式保持普通字段语义。 */
    private static String sequenceName(String expression, String tableSchema) {
        if (expression == null) {
            return null;
        }
        String text = stripOuterParentheses(expression);
        Matcher postgres = POSTGRES_SEQUENCE_DEFAULT.matcher(text);
        if (postgres.matches()) {
            return sequenceIdentifier(postgres.group(1).replace("''", "'"), tableSchema);
        }
        Matcher nextValue = NEXT_VALUE_SEQUENCE_DEFAULT.matcher(text);
        if (nextValue.matches()) {
            return sequenceIdentifier(nextValue.group(1), tableSchema);
        }
        Matcher oracle = ORACLE_SEQUENCE_DEFAULT.matcher(text);
        return oracle.matches() ? sequenceIdentifier(oracle.group(1), tableSchema) : null;
    }

    private static String sequenceIdentifier(String identifier, String resolutionSchema) {
        String sequence = plainIdentifier(identifier);
        if (sequence == null || resolutionSchema == null) {
            return sequence;
        }
        int separator = sequence.indexOf('.');
        return separator > 0
                && separator == sequence.lastIndexOf('.')
                && sequence.substring(0, separator).equals(resolutionSchema)
                ? sequence.substring(separator + 1)
                : sequence;
    }

    private static String stripOuterParentheses(String expression) {
        String text = expression.trim();
        while (text.length() > 1 && text.charAt(0) == '(' && text.charAt(text.length() - 1) == ')') {
            text = text.substring(1, text.length() - 1).trim();
        }
        return text;
    }

    private static String plainIdentifier(String identifier) {
        String[] parts = identifier.trim().split("\\.", -1);
        for (int index = 0; index < parts.length; index++) {
            parts[index] = unquoteIdentifierPart(parts[index].trim());
        }
        String candidate = String.join(".", parts);
        try {
            return SqlIdentifiers.requireIdentifier(candidate, "sequence name");
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }

    private static String unquoteIdentifierPart(String part) {
        if (part.length() < 2) {
            return part;
        }
        char first = part.charAt(0);
        char last = part.charAt(part.length() - 1);
        if (first == '"' && last == '"') {
            return part.substring(1, part.length() - 1).replace("\"\"", "\"");
        }
        if (first == '[' && last == ']') {
            return part.substring(1, part.length() - 1).replace("]]", "]");
        }
        if (first == '`' && last == '`') {
            return part.substring(1, part.length() - 1).replace("``", "`");
        }
        return part;
    }

    private static List<IndexMetadata> toIndexes(List<? extends Map<String, Object>> rows) {
        Map<String, IndexAccumulator> indexes = new LinkedHashMap<>();
        for (Map<String, Object> row : rows) {
            String name = text(row, "INDEX_NAME");
            if (value(row, "INDEX_REPRESENTABLE") != null && !bool(row, "INDEX_REPRESENTABLE")) {
                String reason = optionalText(row, "UNSUPPORTED_INDEX_REASON");
                throw new IllegalStateException("index metadata cannot be represented safely: " + name
                                                        + (reason == null ? "" : " (" + reason + ")"));
            }
            IndexAccumulator index = indexes.computeIfAbsent(name,
                                                             indexName -> new IndexAccumulator(indexName,
                                                                                               bool(row, "UNIQUE_INDEX")));
            index.columns().add(text(row, "COLUMN_NAME"));
        }
        return indexes.values().stream().map(IndexAccumulator::toMetadata).toList();
    }

    private static List<ForeignKeyMetadata> toForeignKeys(List<? extends Map<String, Object>> rows) {
        Map<String, ForeignKeyAccumulator> foreignKeys = new LinkedHashMap<>();
        for (Map<String, Object> row : rows) {
            String name = text(row, "FOREIGN_KEY_NAME");
            ForeignKeyAccumulator foreignKey = foreignKeys.computeIfAbsent(
                    name, key -> new ForeignKeyAccumulator(key, referenceTable(row)));
            foreignKey.columns().add(text(row, "COLUMN_NAME"));
            foreignKey.referenceColumns().add(text(row, "REFERENCED_COLUMN_NAME"));
        }
        return foreignKeys.values().stream().map(ForeignKeyAccumulator::toMetadata).toList();
    }

    private static String referenceTable(Map<String, Object> row) {
        String table = text(row, "REFERENCED_TABLE_NAME");
        String tableSchema = optionalText(row, "TABLE_SCHEMA");
        String referencedTableSchema = optionalText(row, "REFERENCED_TABLE_SCHEMA");
        return tableSchema == null || referencedTableSchema == null || tableSchema.equals(referencedTableSchema)
                ? table
                : referencedTableSchema + "." + table;
    }

    private static DynamicField applyTypeArguments(DynamicField field, Map<String, Object> row) {
        if (switch (field.databaseType().baseName()) {
            case "VARCHAR", "BLOB", "MYSQL_BINARY" -> true;
            default -> false;
        }) {
            return field.withLength(integer(row, "CHARACTER_MAXIMUM_LENGTH", 1));
        }
        if (field.databaseType().logicalType().numeric()
                && ("DECIMAL".equals(field.databaseType().baseName())
                    || "NUMERIC".equals(field.databaseType().baseName()))) {
            return field.withPrecision(integer(row, "NUMERIC_PRECISION", 1),
                                       integer(row, "NUMERIC_SCALE", 0));
        }
        if (switch (field.databaseType().logicalType()) {
            case TIME, OFFSET_TIME, TIMESTAMP, OFFSET_TIMESTAMP -> true;
            default -> false;
        }) {
            return field.withPrecision(temporalPrecision(row), null);
        }
        return field;
    }

    private static Integer temporalPrecision(Map<String, Object> row) {
        Integer precision = integer(row, "TEMPORAL_PRECISION", 0);
        if (precision != null) {
            return precision;
        }
        String dataType = optionalText(row, "DATA_TYPE");
        if (dataType == null) {
            return null;
        }
        List<String> arguments = DatabaseType.of(dataType).arguments();
        if (arguments.isEmpty()) {
            return null;
        }
        try {
            int parsed = Integer.parseInt(arguments.getFirst());
            return parsed < 0 ? null : parsed;
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static String text(Map<String, Object> row, String key) {
        return requireText(optionalText(row, key), key);
    }

    private static String optionalText(Map<String, Object> row, String key) {
        Object value = value(row, key);
        if (value == null) {
            return null;
        }
        String text = value.toString().trim();
        return text.isEmpty() ? null : text;
    }

    private static boolean bool(Map<String, Object> row, String key) {
        Object value = value(row, key);
        if (value instanceof Boolean booleanValue) {
            return booleanValue;
        }
        if (value instanceof Number numericValue) {
            return numericValue.longValue() != 0L;
        }
        if (value == null) {
            return false;
        }
        return switch (value.toString().trim().toLowerCase(Locale.ROOT)) {
            case "1", "true", "yes", "y", "on" -> true;
            default -> false;
        };
    }

    private static boolean nullable(Map<String, Object> row) {
        return value(row, "NULLABLE") == null || bool(row, "NULLABLE");
    }

    private static Integer integer(Map<String, Object> row, String key, int minimum) {
        Object value = value(row, key);
        if (value == null) {
            return null;
        }
        try {
            int number = new BigDecimal(value.toString().trim()).intValueExact();
            return number < minimum ? null : number;
        } catch (NumberFormatException | ArithmeticException error) {
            return null;
        }
    }

    private static Object value(Map<String, Object> row, String key) {
        Object value = row.get(key);
        return value == null ? row.get(key.toLowerCase(Locale.ROOT)) : value;
    }

    static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }

    private record IndexAccumulator(String name, boolean unique, List<String> columns) {

        private IndexAccumulator(String name, boolean unique) {
            this(name, unique, new ArrayList<>());
        }

        private IndexMetadata toMetadata() {
            IndexMetadata.Builder builder = IndexMetadata.builder(name);
            if (unique) {
                builder.unique();
            }
            columns.forEach(builder::addColumn);
            return builder.build();
        }
    }

    private record ForeignKeyAccumulator(String name,
                                         String referenceTable,
                                         List<String> columns,
                                         List<String> referenceColumns) {

        private ForeignKeyAccumulator(String name, String referenceTable) {
            this(name, referenceTable, new ArrayList<>(), new ArrayList<>());
        }

        private ForeignKeyMetadata toMetadata() {
            ForeignKeyMetadata.Builder builder = ForeignKeyMetadata.builder(name)
                                                                   .referenceTable(referenceTable);
            columns.forEach(builder::addColumn);
            referenceColumns.forEach(builder::addReferenceColumn);
            return builder.build();
        }
    }
}
