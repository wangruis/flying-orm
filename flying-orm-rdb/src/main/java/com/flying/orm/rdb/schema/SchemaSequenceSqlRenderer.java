package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlRequest;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/** 只负责命名序列 DDL 的去重、复用和删除，避免表 DDL 渲染器承担序列身份规则。 */
final class SchemaSequenceSqlRenderer {

    private final SchemaDialect dialect;
    private final Function<DynamicField, String> dataTypeRenderer;

    SchemaSequenceSqlRenderer(SchemaDialect dialect, Function<DynamicField, String> dataTypeRenderer) {
        this.dialect = Objects.requireNonNull(dialect, "schema dialect must not be null");
        this.dataTypeRenderer = Objects.requireNonNull(dataTypeRenderer, "data type renderer must not be null");
    }

    void addCreates(List<SqlRequest> requests,
                    List<DynamicField> fields,
                    List<DynamicField> existingFields,
                    boolean verifyExistingDefinitions) {
        List<String> existingSequences = sequenceNames(existingFields);
        Map<String, Definition> existingDefinitions = verifyExistingDefinitions
                ? sequenceCreates(existingFields) : Map.of();
        Map<String, Definition> sequences = sequenceCreates(fields, existingDefinitions);
        sequences.keySet().removeAll(existingSequences);
        sequences.values().forEach(definition -> requests.add(new SqlRequest(definition.sql(), List.of())));
    }

    void addCreate(List<SqlRequest> requests, DynamicField field) {
        dialect.createSequenceSql(field.generation(), dataTypeRenderer.apply(field))
               .ifPresent(sql -> requests.add(new SqlRequest(sql, List.of())));
    }

    static void addCreate(SchemaDialect dialect,
                          List<SqlRequest> requests,
                          RelationIdentity relation,
                          ColumnDefinition column,
                          Map<String, Definition> existingSequences,
                          Map<String, Definition> createdSequences,
                          Function<ColumnDefinition, String> dataTypeRenderer) {
        if (column.generation().strategy() != ValueGeneration.Strategy.SEQUENCE) {
            return;
        }
        requireQualifiedSequence(relation, column.generation());
        String dataType = dataTypeRenderer.apply(column);
        dialect.createSequenceSql(column.generation(), dataType).ifPresent(sql -> {
            String name = column.generation().sequenceName();
            String sequenceKey = SchemaDefinitionEquality.canonicalName(name, dialect);
            // Compare definitions after name alignment, but emit DDL with the desired spelling.
            Definition definition = definition(dialect,
                    sequenceGeneration(column.generation(), sequenceKey), dataType);
            Definition existing = existingSequences.get(sequenceKey);
            if (existing != null) {
                existing.requireCompatible(dialect, definition);
            }
            if (existing == null && collect(createdSequences, sequenceKey, definition)) {
                requests.add(new SqlRequest(sql, List.of()));
            }
        });
    }

    private static void requireQualifiedSequence(RelationIdentity relation, ValueGeneration generation) {
        if (relation.schema().isPresent() && generation.sequenceName().indexOf('.') < 0) {
            throw new IllegalArgumentException(
                    "sequence name must include its schema for a schema-qualified relation: "
                            + generation.sequenceName());
        }
    }

    static Map<String, Definition> definitions(SchemaDialect dialect,
                                           List<SchemaSnapshot> snapshots,
                                           Function<ColumnDefinition, String> dataTypeRenderer) {
        Map<String, Definition> definitions = new LinkedHashMap<>();
        for (SchemaSnapshot snapshot : snapshots) {
            for (ColumnDefinition column : snapshot.columns().optionalValue().orElse(List.of())) {
                if (column.generation().strategy() != ValueGeneration.Strategy.SEQUENCE) {
                    continue;
                }
                String observedName = SchemaDefinitionEquality.observedSequenceName(
                        column.generation().sequenceName(), snapshot.identity());
                // Only an observed bare name inherits its table schema; desired bare names stay untouched.
                String sequenceKey = SchemaDefinitionEquality.canonicalName(observedName, dialect);
                collect(definitions, sequenceKey, definition(dialect,
                        sequenceGeneration(column.generation(), sequenceKey), dataTypeRenderer.apply(column)));
            }
        }
        return definitions;
    }

    List<SqlRequest> drops(List<DynamicField> fields, List<DynamicField> retainedFields) {
        List<String> retainedSequences = sequenceNames(retainedFields);
        Map<String, String> sequences = new LinkedHashMap<>();
        for (DynamicField field : requireFields(fields)) {
            dialect.dropSequenceSql(field.generation()).ifPresent(sql ->
                    sequences.putIfAbsent(field.generation().sequenceName(), sql));
        }
        sequences.keySet().removeIf(retainedSequences::contains);
        return sequences.values().stream().map(sql -> new SqlRequest(sql, List.of())).toList();
    }

    private Map<String, Definition> sequenceCreates(List<DynamicField> fields) {
        return sequenceCreates(fields, Map.of());
    }

    private Map<String, Definition> sequenceCreates(List<DynamicField> fields, Map<String, Definition> existing) {
        Map<String, Definition> sequences = new LinkedHashMap<>();
        for (DynamicField field : requireFields(fields)) {
            String dataType = dataTypeRenderer.apply(field);
            String sql = dialect.createSequenceSql(field.generation(), dataType).orElse(null);
            if (sql == null) {
                continue;
            }
            String sequenceName = field.generation().sequenceName();
            Definition definition = new Definition(field.generation(), dataType, sql);
            Definition previous = existing.get(sequenceName);
            if (previous != null) {
                previous.requireCompatible(dialect, definition);
            } else {
                collect(sequences, sequenceName, definition);
            }
        }
        return sequences;
    }

    private static boolean collect(Map<String, Definition> sequences, String sequenceName, Definition definition) {
        Definition previous = sequences.putIfAbsent(sequenceName, definition);
        requireSameDefinition(sequenceName, previous == null ? null : previous.sql(), definition.sql());
        return previous == null;
    }

    static void requireSameDefinition(String sequenceName, String previous, String current) {
        if (previous != null && !previous.equals(current)) {
            throw new IllegalArgumentException("sequence is declared with different options: " + sequenceName);
        }
    }

    private static Definition definition(SchemaDialect dialect, ValueGeneration generation, String dataType) {
        return new Definition(generation, dataType, dialect.createSequenceSql(generation, dataType).orElseThrow());
    }

    /** 保留已渲染定义及其选项；未指定缓存只放宽已有序列的缓存匹配，不合并相互冲突的新声明。 */
    record Definition(ValueGeneration generation, String dataType, String sql) {

        private void requireCompatible(SchemaDialect dialect, Definition desired) {
            String expected = desired.sql();
            if (desired.generation().cacheSize() == 0 && generation.cacheSize() != 0) {
                ValueGeneration effective = ValueGeneration.sequence(desired.generation().sequenceName(),
                        desired.generation().startWith(), desired.generation().incrementBy(), generation.cacheSize());
                expected = dialect.createSequenceSql(effective, desired.dataType()).orElseThrow();
            }
            requireSameDefinition(desired.generation().sequenceName(), sql, expected);
        }
    }

    private static ValueGeneration sequenceGeneration(ValueGeneration generation, String sequenceName) {
        return ValueGeneration.sequence(
                sequenceName, generation.startWith(), generation.incrementBy(), generation.cacheSize());
    }

    private static List<String> sequenceNames(List<DynamicField> fields) {
        return requireFields(fields).stream()
                                    .map(DynamicField::generation)
                                    .filter(generation -> generation.strategy()
                                            == ValueGeneration.Strategy.SEQUENCE)
                                    .map(ValueGeneration::sequenceName)
                                    .toList();
    }

    private static List<DynamicField> requireFields(List<DynamicField> fields) {
        return Objects.requireNonNull(fields, "dynamic fields must not be null");
    }
}
