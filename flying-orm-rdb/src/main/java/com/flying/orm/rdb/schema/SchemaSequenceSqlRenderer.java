package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
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
            String previous = sequences.putIfAbsent(sequenceName, sql);
            requireSameDefinition(sequenceName, previous, sql);
        }
        return sequences;
    }

    private static void requireSameDefinition(String sequenceName, String previous, String current) {
        if (previous != null && !previous.equals(current)) {
            throw new IllegalArgumentException("sequence is declared with different options: " + sequenceName);
        }
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
