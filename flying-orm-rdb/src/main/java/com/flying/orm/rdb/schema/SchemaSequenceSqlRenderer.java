package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.metadata.ColumnDefinition;
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
        Map<String, String> existingDefinitions = verifyExistingDefinitions
                ? sequenceCreates(existingFields) : Map.of();
        Map<String, String> sequences = sequenceCreates(fields);
        sequences.entrySet().removeIf(entry -> {
            if (!existingSequences.contains(entry.getKey())) {
                return false;
            }
            if (verifyExistingDefinitions) {
                requireSameDefinition(entry.getKey(), existingDefinitions.get(entry.getKey()), entry.getValue());
            }
            return true;
        });
        sequences.values().forEach(sql -> requests.add(new SqlRequest(sql, List.of())));
    }

    void addCreate(List<SqlRequest> requests, DynamicField field) {
        dialect.createSequenceSql(field.generation(), dataTypeRenderer.apply(field))
               .ifPresent(sql -> requests.add(new SqlRequest(sql, List.of())));
    }

    static void addCreate(SchemaDialect dialect,
                          List<SqlRequest> requests,
                          ColumnDefinition column,
                          Map<String, String> existingSequences,
                          Map<String, String> createdSequences,
                          Function<ColumnDefinition, String> dataTypeRenderer) {
        if (column.generation().strategy() != ValueGeneration.Strategy.SEQUENCE) {
            return;
        }
        String dataType = dataTypeRenderer.apply(column);
        dialect.createSequenceSql(column.generation(), dataType).ifPresent(sql -> {
            String name = column.generation().sequenceName();
            String sequenceKey = SchemaDefinitionEquality.canonicalName(name, dialect);
            // Compare definitions after name alignment, but emit DDL with the desired spelling.
            String definition = dialect.createSequenceSql(
                    sequenceGeneration(column.generation(), sequenceKey), dataType).orElseThrow();
            String existing = existingSequences.get(sequenceKey);
            requireSameDefinition(name, existing, definition);
            if (existing == null && collect(createdSequences, sequenceKey, definition)) {
                requests.add(new SqlRequest(sql, List.of()));
            }
        });
    }

    static Map<String, String> definitions(SchemaDialect dialect,
                                           List<SchemaSnapshot> snapshots,
                                           Function<ColumnDefinition, String> dataTypeRenderer) {
        Map<String, String> definitions = new LinkedHashMap<>();
        for (SchemaSnapshot snapshot : snapshots) {
            for (ColumnDefinition column : snapshot.columns().optionalValue().orElse(List.of())) {
                if (column.generation().strategy() != ValueGeneration.Strategy.SEQUENCE) {
                    continue;
                }
                String observedName = SchemaDefinitionEquality.observedSequenceName(
                        column.generation().sequenceName(), snapshot.identity());
                // Only an observed bare name inherits its table schema; desired bare names stay untouched.
                String sequenceKey = SchemaDefinitionEquality.canonicalName(observedName, dialect);
                dialect.createSequenceSql(
                        sequenceGeneration(column.generation(), sequenceKey),
                        dataTypeRenderer.apply(column)).ifPresent(sql ->
                        collect(definitions, sequenceKey, sql));
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

    private Map<String, String> sequenceCreates(List<DynamicField> fields) {
        Map<String, String> sequences = new LinkedHashMap<>();
        for (DynamicField field : requireFields(fields)) {
            String sql = dialect.createSequenceSql(field.generation(), dataTypeRenderer.apply(field)).orElse(null);
            if (sql == null) {
                continue;
            }
            String sequenceName = field.generation().sequenceName();
            collect(sequences, sequenceName, sql);
        }
        return sequences;
    }

    static boolean collect(Map<String, String> sequences, String sequenceName, String sql) {
        String previous = sequences.putIfAbsent(sequenceName, sql);
        requireSameDefinition(sequenceName, previous, sql);
        return previous == null;
    }

    static void requireSameDefinition(String sequenceName, String previous, String current) {
        if (previous != null && !previous.equals(current)) {
            throw new IllegalArgumentException("sequence is declared with different options: " + sequenceName);
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
