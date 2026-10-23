package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlRequest;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

/** 负责命名序列 DDL 的去重和复用，避免表 DDL 渲染器承担序列身份规则。 */
final class SchemaSequenceSqlRenderer {

    private SchemaSequenceSqlRenderer() {
    }

    static Map<String, Definition> definitions(SchemaDialect dialect,
                                               List<ColumnDefinition> existingFields,
                                               Function<ColumnDefinition, String> dataTypeRenderer,
                                               boolean verifyExistingDefinitions) {
        Map<String, Definition> existing = new LinkedHashMap<>();
        for (ColumnDefinition field : existingFields) {
            if (field.generation().strategy() != ValueGeneration.Strategy.SEQUENCE) continue;
            String key = sequenceKey(dialect, field.generation().sequenceName());
            ValueGeneration generation = sequenceGeneration(field.generation(), key);
            Definition observed = verifyExistingDefinitions
                    ? definition(dialect, generation, dataTypeRenderer.apply(field), true)
                    : new Definition(generation, null, null, true);
            collect(existing, key, observed);
        }
        return existing;
    }

    static void addCreate(SchemaDialect dialect,
                          List<SqlRequest> requests,
                          ColumnDefinition column,
                          Map<String, Definition> existingSequences,
                          Map<String, Definition> createdSequences,
                          Function<ColumnDefinition, String> dataTypeRenderer) {
        if (column.generation().strategy() != ValueGeneration.Strategy.SEQUENCE) {
            return;
        }
        prepare(dialect, requests, column, existingSequences, createdSequences, dataTypeRenderer);
    }

    private static void prepare(SchemaDialect dialect, List<SqlRequest> requests, ColumnDefinition column,
                                Map<String, Definition> existingSequences, Map<String, Definition> declarations,
                                Function<ColumnDefinition, String> dataTypeRenderer) {
        String dataType = dataTypeRenderer.apply(column);
        dialect.createSequenceSql(column.generation(), dataType).ifPresent(sql -> {
            String name = column.generation().sequenceName();
            String sequenceKey = sequenceKey(dialect, name);
            // Compare definitions after name alignment, but emit DDL with the desired spelling.
            Definition definition = definition(dialect,
                    sequenceGeneration(column.generation(), sequenceKey), dataType, false);
            Definition existing = existingSequences.get(sequenceKey);
            if (existing != null && existing.sql() != null) {
                existing.requireCompatible(dialect, definition);
            } else if (collect(declarations, sequenceKey, definition) && existing == null) {
                requests.add(new SqlRequest(sql, List.of()));
            }
        });
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
                String sequenceKey = sequenceKey(dialect, observedName);
                collect(definitions, sequenceKey, definition(dialect,
                        sequenceGeneration(column.generation(), sequenceKey), dataTypeRenderer.apply(column), true));
            }
        }
        return definitions;
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

    private static Definition definition(SchemaDialect dialect, ValueGeneration generation, String dataType,
                                         boolean observed) {
        return new Definition(generation, dataType, dialect.createSequenceSql(generation, dataType).orElseThrow(), observed);
    }

    /** observed 区分数据库事实与本次声明；sql 为 null 表示只观察到了身份，参数未知。 */
    record Definition(ValueGeneration generation, String dataType, String sql, boolean observed) {

        private void requireCompatible(SchemaDialect dialect, Definition desired) {
            if (!observed) {
                requireSameDefinition(generation.sequenceName(), sql, desired.sql());
                return;
            }
            String expected = desired.sql();
            long start = SchemaGeneratedValueComparison.hasObservableStart(dialect.generatedValueStyle(), generation)
                    ? desired.generation().startWith() : generation.startWith();
            int cache = desired.generation().cacheSize() == 0 ? generation.cacheSize() : desired.generation().cacheSize();
            if (start != desired.generation().startWith() || cache != desired.generation().cacheSize()) {
                ValueGeneration effective = ValueGeneration.sequence(desired.generation().sequenceName(),
                        start, desired.generation().incrementBy(), cache);
                expected = dialect.createSequenceSql(effective, desired.dataType()).orElseThrow();
            }
            requireSameDefinition(desired.generation().sequenceName(), sql, expected);
        }
    }

    private static ValueGeneration sequenceGeneration(ValueGeneration generation, String sequenceName) {
        return ValueGeneration.sequence(
                sequenceName, generation.startWith(), generation.incrementBy(), generation.cacheSize());
    }

    private static String sequenceKey(SchemaDialect dialect, String name) {
        if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.POSTGRESQL
                && dialect.identifier(name).equals(name)) {
            return name.toLowerCase(Locale.ROOT);
        }
        return SchemaDefinitionEquality.canonicalName(name, dialect);
    }

}
