package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.ReferentialAction;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.metadata.TablePartitionDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlIdentifiers;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.schema.SchemaSnapshot;

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

    /**
     * 把同一次字典读取转成三态 Schema 快照。空列结果明确表示表不存在；未配置的附属查询保持
     * UNKNOWN，不能与“查询过且没有结果”混为一谈。
     */
    static SchemaSnapshot toSchemaSnapshot(RelationIdentity identity,
                                           String displayTable,
                                           List<? extends Map<String, Object>> columnRows,
                                           List<? extends Map<String, Object>> indexRows,
                                           boolean indexesObserved,
                                           List<? extends Map<String, Object>> foreignKeyRows,
                                           boolean foreignKeysObserved,
                                           Function<String, String> typeMapper) {
        Objects.requireNonNull(columnRows, "metadata column rows must not be null");
        if (columnRows.isEmpty()) {
            return SchemaSnapshot.absent(identity);
        }
        DynamicForm form = toDynamicForm(displayTable, displayTable, columnRows, typeMapper);
        TableMetadata table = toTableMetadata(displayTable, form, indexRows, foreignKeyRows);
        return SchemaSnapshot.fromLegacy(identity, table, indexesObserved, foreignKeysObserved);
    }

    /**
     * 把已经由方言查询归一化的完整字典结果直接装配成关系快照。
     *
     * <p>这条路径不经过旧 {@link TableMetadata}，因为旧模型没有命名主键、唯一约束、CHECK、
     * 默认值和外键动作。任何无法落入封闭关系模型的数据库事实都会在这里明确失败，调用方不会
     * 得到一个虚假的 complete 快照。</p>
     */
    static SchemaSnapshot toCompleteSchemaSnapshot(
            RelationIdentity identity,
            List<? extends Map<String, Object>> columnRows,
            List<? extends Map<String, Object>> tableRows,
            List<? extends Map<String, Object>> primaryKeyRows,
            List<? extends Map<String, Object>> uniqueRows,
            List<? extends Map<String, Object>> indexRows,
            List<? extends Map<String, Object>> foreignKeyRows,
            List<? extends Map<String, Object>> checkRows,
            Function<String, String> typeMapper,
            InformationSchemaFormMetadataReader.SnapshotDialect dialect) {
        Objects.requireNonNull(columnRows, "metadata column rows must not be null");
        Objects.requireNonNull(tableRows, "metadata table rows must not be null");
        if (tableRows.isEmpty() && columnRows.isEmpty()) {
            return SchemaSnapshot.absent(identity);
        }
        if (tableRows.size() != 1 || columnRows.isEmpty()) {
            throw new IllegalStateException("table existence and column metadata are inconsistent");
        }
        List<ColumnDefinition> columns = toColumnDefinitions(columnRows, typeMapper, dialect);
        SchemaSnapshot.Builder snapshot = SchemaSnapshot.builder(identity)
                .tablePresent()
                .columns(columns)
                .uniqueConstraints(toUniqueConstraints(uniqueRows))
                .indexes(toIndexDefinitions(indexRows))
                .foreignKeys(toForeignKeyDefinitions(identity, foreignKeyRows))
                .checks(toCheckConstraints(checkRows, columns, dialect));
        applyTableFacts(snapshot, tableRows, dialect);
        PrimaryKeyDefinition primaryKey = toPrimaryKey(primaryKeyRows);
        if (primaryKey == null) {
            snapshot.primaryKeyAbsent();
        } else {
            snapshot.primaryKey(primaryKey);
        }
        return snapshot.build();
    }

    private static List<ColumnDefinition> toColumnDefinitions(
            List<? extends Map<String, Object>> rows,
            Function<String, String> typeMapper,
            InformationSchemaFormMetadataReader.SnapshotDialect dialect) {
        Function<String, String> safeTypeMapper = Objects.requireNonNull(
                typeMapper, "type mapper must not be null");
        List<ColumnDefinition> columns = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
            requireRepresentable(row, "COLUMN_REPRESENTABLE", "UNSUPPORTED_COLUMN_REASON",
                                 "column", text(row, "COLUMN_NAME"));
            String mappedType = safeTypeMapper.apply(text(row, "DATA_TYPE"));
            DatabaseType databaseType = DatabaseType.of(mappedType);
            ValueGeneration generation = generation(row);
            ColumnDefinition.Builder column = ColumnDefinition.builder(text(row, "COLUMN_NAME"), databaseType)
                    .nullable(nullable(row))
                    .comment(optionalText(row, "REMARKS"))
                    .generation(generation)
                    .charset(optionalText(row, "COLUMN_CHARSET"))
                    .collation(optionalText(row, "COLUMN_COLLATION"));
            applyTypeArguments(column, databaseType, row);
            if (!generation.generated()) {
                column.defaultValue(RelationalMetadataValueParser.columnDefault(
                        nullableRawText(row, "COLUMN_DEFAULT"), databaseType, dialect));
            }
            columns.add(column.build());
        }
        return List.copyOf(columns);
    }

    private static void applyTypeArguments(ColumnDefinition.Builder column,
                                           DatabaseType databaseType,
                                           Map<String, Object> row) {
        if (switch (databaseType.baseName()) {
            case "VARCHAR", "BLOB", "MYSQL_BINARY" -> true;
            default -> false;
        }) {
            column.length(integer(row, "CHARACTER_MAXIMUM_LENGTH", 1));
            return;
        }
        if (databaseType.logicalType().numeric()
                && ("DECIMAL".equals(databaseType.baseName())
                    || "NUMERIC".equals(databaseType.baseName()))) {
            column.precision(integer(row, "NUMERIC_PRECISION", 1))
                  .scale(integer(row, "NUMERIC_SCALE", 0));
            return;
        }
        if (databaseType.isTemporal()) {
            column.temporalPrecision(temporalPrecision(row));
        }
    }

    private static ValueGeneration generation(Map<String, Object> row) {
        if (bool(row, "IS_IDENTITY")) {
            return ValueGeneration.identity(
                    generationLong(row, "GENERATION_START", 1L),
                    generationLong(row, "GENERATION_INCREMENT", 1L),
                    generationCache(row, 100));
        }
        String sequence = sequenceName(optionalText(row, "GENERATION_EXPRESSION"),
                                       optionalText(row, "RESOLUTION_SCHEMA"));
        if (sequence == null) {
            return ValueGeneration.none();
        }
        String declaredSequenceText = optionalText(row, "GENERATION_SEQUENCE_NAME");
        String declaredSequence = declaredSequenceText == null ? null : sequenceIdentifier(
                declaredSequenceText, optionalText(row, "RESOLUTION_SCHEMA"));
        if (declaredSequence != null && !declaredSequence.equals(sequence)) {
            throw new IllegalStateException(
                    "sequence generation metadata does not match the column default");
        }
        requireSequenceOption(row, "GENERATION_START");
        requireSequenceOption(row, "GENERATION_INCREMENT");
        requireSequenceOption(row, "GENERATION_CACHE");
        return ValueGeneration.sequence(
                sequence,
                generationLong(row, "GENERATION_START", 1L),
                generationLong(row, "GENERATION_INCREMENT", 1L),
                generationCache(row, 100));
    }

    private static void requireSequenceOption(Map<String, Object> row, String key) {
        if (value(row, key) == null) {
            throw new IllegalStateException("sequence generation metadata is incomplete: " + key);
        }
    }

    private static long generationLong(Map<String, Object> row, String key, long defaultValue) {
        Object raw = value(row, key);
        if (raw == null) {
            return defaultValue;
        }
        try {
            return new BigDecimal(raw.toString().trim()).longValueExact();
        } catch (NumberFormatException | ArithmeticException error) {
            throw new IllegalStateException("invalid generated-value option: " + key, error);
        }
    }

    private static int generationCache(Map<String, Object> row, int defaultValue) {
        long cache = generationLong(row, "GENERATION_CACHE", defaultValue);
        if (cache < 0 || cache > Integer.MAX_VALUE) {
            throw new IllegalStateException("invalid generated-value option: GENERATION_CACHE");
        }
        return (int) cache;
    }

    private static void applyTableFacts(
            SchemaSnapshot.Builder snapshot,
            List<? extends Map<String, Object>> rows,
            InformationSchemaFormMetadataReader.SnapshotDialect dialect) {
        Objects.requireNonNull(rows, "metadata table rows must not be null");
        if (rows.size() != 1) {
            throw new IllegalStateException("present table metadata must contain exactly one table row");
        }
        Map<String, Object> row = rows.getFirst();
        requireRepresentable(
                row, "TABLE_REPRESENTABLE", "UNSUPPORTED_TABLE_REASON", "table", "<current>");
        String comment = optionalText(row, "TABLE_COMMENT");
        if (comment == null) {
            snapshot.tableCommentAbsent();
        } else {
            snapshot.tableComment(comment);
        }
        applyTablePartition(snapshot, row, dialect);
    }

    private static void applyTablePartition(
            SchemaSnapshot.Builder snapshot,
            Map<String, Object> row,
            InformationSchemaFormMetadataReader.SnapshotDialect dialect) {
        if (dialect != InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL
                || !bool(row, "TABLE_PARTITIONED")) {
            snapshot.partitionAbsent();
            return;
        }
        String strategy = optionalText(row, "PARTITION_STRATEGY");
        String column = optionalText(row, "PARTITION_COLUMN");
        if (!"RANGE".equals(strategy) || column == null) {
            throw new IllegalStateException("partition metadata cannot be represented safely");
        }
        snapshot.partition(TablePartitionDefinition.range(column));
    }

    private static PrimaryKeyDefinition toPrimaryKey(List<? extends Map<String, Object>> rows) {
        rows.forEach(row -> requireRepresentable(
                row, "CONSTRAINT_REPRESENTABLE", null,
                "primary-key constraint", text(row, "CONSTRAINT_NAME")));
        Map<String, List<String>> constraints = namedColumns(rows);
        if (constraints.isEmpty()) {
            return null;
        }
        if (constraints.size() != 1) {
            throw new IllegalStateException("table metadata contains more than one primary key");
        }
        Map.Entry<String, List<String>> entry = constraints.entrySet().iterator().next();
        return new PrimaryKeyDefinition(entry.getKey(), entry.getValue());
    }

    private static List<UniqueConstraintDefinition> toUniqueConstraints(
            List<? extends Map<String, Object>> rows) {
        rows.forEach(row -> requireRepresentable(
                row, "CONSTRAINT_REPRESENTABLE", null,
                "unique constraint", text(row, "CONSTRAINT_NAME")));
        return namedColumns(rows).entrySet().stream()
                .map(entry -> new UniqueConstraintDefinition(entry.getKey(), entry.getValue()))
                .toList();
    }

    private static Map<String, List<String>> namedColumns(List<? extends Map<String, Object>> rows) {
        Objects.requireNonNull(rows, "constraint metadata rows must not be null");
        Map<String, List<String>> constraints = new LinkedHashMap<>();
        for (Map<String, Object> row : rows) {
            constraints.computeIfAbsent(text(row, "CONSTRAINT_NAME"), ignored -> new ArrayList<>())
                       .add(text(row, "COLUMN_NAME"));
        }
        return constraints;
    }

    private static List<IndexDefinition> toIndexDefinitions(
            List<? extends Map<String, Object>> rows) {
        Map<String, RelationalIndexAccumulator> indexes = new LinkedHashMap<>();
        for (Map<String, Object> row : Objects.requireNonNull(rows, "index metadata rows must not be null")) {
            String name = text(row, "INDEX_NAME");
            requireRepresentable(row, "INDEX_REPRESENTABLE", "UNSUPPORTED_INDEX_REASON", "index", name);
            boolean unique = bool(row, "UNIQUE_INDEX");
            RelationalIndexAccumulator index = indexes.computeIfAbsent(
                    name, ignored -> new RelationalIndexAccumulator(name, unique));
            if (index.unique != unique) {
                throw new IllegalStateException("index metadata changes uniqueness inside one index");
            }
            String direction = text(row, "INDEX_DIRECTION").toUpperCase(Locale.ROOT);
            String column = indexColumn(row, direction);
            index.keys.add(switch (direction) {
                case "ASC" -> IndexKeyPart.asc(column);
                case "DESC" -> IndexKeyPart.desc(column);
                default -> throw new IllegalStateException("unsupported index direction");
            });
        }
        return indexes.values().stream().map(RelationalIndexAccumulator::build).toList();
    }

    private static String indexColumn(Map<String, Object> row, String direction) {
        String expression = optionalText(row, "INDEX_EXPRESSION");
        if (expression == null) {
            return text(row, "COLUMN_NAME");
        }
        if (!"DESC".equals(direction) || expression.length() < 2
                || expression.charAt(0) != '"' || expression.charAt(expression.length() - 1) != '"') {
            throw new IllegalStateException("index expression cannot be represented safely");
        }
        StringBuilder identifier = new StringBuilder(expression.length() - 2);
        for (int index = 1; index < expression.length() - 1; index++) {
            char current = expression.charAt(index);
            if (current != '"') {
                identifier.append(current);
                continue;
            }
            if (index + 1 >= expression.length() - 1 || expression.charAt(index + 1) != '"') {
                throw new IllegalStateException("index expression cannot be represented safely");
            }
            identifier.append('"');
            index++;
        }
        if (identifier.isEmpty()) {
            throw new IllegalStateException("index expression cannot be represented safely");
        }
        return identifier.toString();
    }

    private static List<ForeignKeyDefinition> toForeignKeyDefinitions(
            RelationIdentity source,
            List<? extends Map<String, Object>> rows) {
        Map<String, RelationalForeignKeyAccumulator> foreignKeys = new LinkedHashMap<>();
        for (Map<String, Object> row : Objects.requireNonNull(rows, "foreign-key metadata rows must not be null")) {
            String name = text(row, "FOREIGN_KEY_NAME");
            requireRepresentable(row, "CONSTRAINT_REPRESENTABLE", null,
                                 "foreign-key constraint", name);
            RelationIdentity reference = referenceIdentity(source, row);
            ReferentialAction onDelete = action(row, "ON_DELETE");
            ReferentialAction onUpdate = action(row, "ON_UPDATE");
            RelationalForeignKeyAccumulator foreignKey = foreignKeys.computeIfAbsent(
                    name, ignored -> new RelationalForeignKeyAccumulator(name, reference, onDelete, onUpdate));
            foreignKey.requireSame(reference, onDelete, onUpdate);
            foreignKey.columns.add(text(row, "COLUMN_NAME"));
            foreignKey.referenceColumns.add(text(row, "REFERENCED_COLUMN_NAME"));
        }
        return foreignKeys.values().stream().map(RelationalForeignKeyAccumulator::build).toList();
    }

    private static RelationIdentity referenceIdentity(RelationIdentity source, Map<String, Object> row) {
        String table = text(row, "REFERENCED_TABLE_NAME");
        String localSchema = optionalText(row, "TABLE_SCHEMA");
        String schema = optionalText(row, "REFERENCED_TABLE_SCHEMA");
        String localCatalog = optionalText(row, "TABLE_CATALOG");
        String catalog = optionalText(row, "REFERENCED_TABLE_CATALOG");
        if (source.schema().isEmpty() && Objects.equals(localSchema, schema)) {
            schema = null;
        }
        if (source.catalog().isEmpty() && Objects.equals(localCatalog, catalog)) {
            catalog = null;
        }
        return RelationIdentity.of(catalog, schema, table);
    }

    private static ReferentialAction action(Map<String, Object> row, String key) {
        String value = text(row, key).toUpperCase(Locale.ROOT).replace(' ', '_');
        try {
            return ReferentialAction.valueOf(value);
        } catch (IllegalArgumentException error) {
            throw new IllegalStateException("unsupported foreign-key action", error);
        }
    }

    private static List<CheckConstraintDefinition> toCheckConstraints(
            List<? extends Map<String, Object>> rows,
            List<ColumnDefinition> columns,
            InformationSchemaFormMetadataReader.SnapshotDialect dialect) {
        Map<String, DatabaseType> columnTypes = new LinkedHashMap<>();
        columns.forEach(column -> columnTypes.put(column.name(), column.databaseType()));
        List<CheckConstraintDefinition> checks = new ArrayList<>(rows.size());
        for (Map<String, Object> row : Objects.requireNonNull(rows, "check metadata rows must not be null")) {
            String name = text(row, "CONSTRAINT_NAME");
            requireRepresentable(row, "CHECK_REPRESENTABLE", null, "check constraint", name);
            checks.add(CheckConstraintDefinition.of(
                    name,
                    RelationalMetadataValueParser.checkPredicate(
                            text(row, "CHECK_EXPRESSION"), columnTypes, dialect)));
        }
        return List.copyOf(checks);
    }

    private static void requireRepresentable(Map<String, Object> row,
                                             String flag,
                                             String reasonKey,
                                             String category,
                                             String name) {
        if (value(row, flag) == null || bool(row, flag)) {
            return;
        }
        String reason = reasonKey == null ? null : optionalText(row, reasonKey);
        throw new IllegalStateException(category + " metadata cannot be represented safely: " + name
                                                + (reason == null ? "" : " (" + reason + ")"));
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

    private static String nullableRawText(Map<String, Object> row, String key) {
        Object value = value(row, key);
        return value == null ? null : value.toString();
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

    private static final class RelationalIndexAccumulator {

        private final String name;
        private final boolean unique;
        private final List<IndexKeyPart> keys = new ArrayList<>();

        private RelationalIndexAccumulator(String name, boolean unique) {
            this.name = name;
            this.unique = unique;
        }

        private IndexDefinition build() {
            IndexDefinition.Builder builder = IndexDefinition.builder(name).unique(unique);
            keys.forEach(builder::addKey);
            return builder.build();
        }
    }

    private static final class RelationalForeignKeyAccumulator {

        private final String name;
        private final RelationIdentity reference;
        private final ReferentialAction onDelete;
        private final ReferentialAction onUpdate;
        private final List<String> columns = new ArrayList<>();
        private final List<String> referenceColumns = new ArrayList<>();

        private RelationalForeignKeyAccumulator(String name,
                                                RelationIdentity reference,
                                                ReferentialAction onDelete,
                                                ReferentialAction onUpdate) {
            this.name = name;
            this.reference = reference;
            this.onDelete = onDelete;
            this.onUpdate = onUpdate;
        }

        private void requireSame(RelationIdentity candidate,
                                 ReferentialAction deleteAction,
                                 ReferentialAction updateAction) {
            if (!reference.equals(candidate) || onDelete != deleteAction || onUpdate != updateAction) {
                throw new IllegalStateException("foreign-key metadata changes inside one constraint");
            }
        }

        private ForeignKeyDefinition build() {
            ForeignKeyDefinition.Builder builder = ForeignKeyDefinition.builder(name)
                    .reference(reference)
                    .onDelete(onDelete)
                    .onUpdate(onUpdate);
            columns.forEach(builder::addColumn);
            referenceColumns.forEach(builder::addReferenceColumn);
            return builder.build();
        }
    }
}
