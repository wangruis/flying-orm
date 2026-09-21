package com.flying.orm.rdb.result;

import com.flying.orm.rdb.internal.mapping.EntityFieldNames;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.RowMetadata;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.IntFunction;
import java.util.function.Supplier;

/**
 * 一批查询结果共享的列布局。
 *
 * <p>布局只保存一次列名和索引，不为每行重复创建 Map 节点。八列以内直接扫描数组通常更省内存，
 * 列更多时才保留一次 HashMap 索引。这个对象只跟随当前结果行存活，不进入全局缓存。</p>
 */
final class RowLayout {

    private static final int LINEAR_LOOKUP_LIMIT = 8;

    private final String[] columnNames;

    private final Map<String, Integer> indexes;

    private final String[] mappingKeys;

    private final Map<String, Integer> mappingIndexes;

    private final boolean ambiguousMappingColumns;

    /** 按计划身份保存当前结果布局的字段绑定；生命周期不会超过当前 RowLayout。 */
    private Map<Object, Object> mappingBindings;

    private RowLayout(String[] columnNames,
                      Map<String, Integer> indexes,
                      String[] mappingKeys,
                      Map<String, Integer> mappingIndexes,
                      boolean ambiguousMappingColumns) {
        this.columnNames = columnNames;
        this.indexes = indexes;
        this.mappingKeys = mappingKeys;
        this.mappingIndexes = mappingIndexes;
        this.ambiguousMappingColumns = ambiguousMappingColumns;
    }

    static RowLayout of(List<String> columnNames) {
        List<String> safeNames = Objects.requireNonNull(columnNames, "row column names must not be null");
        return materialize(safeNames.size(), safeNames::get);
    }

    static RowLayout from(RowMetadata metadata) {
        List<? extends ColumnMetadata> columns = Objects.requireNonNull(
                metadata, "row metadata must not be null").getColumnMetadatas();
        return materialize(columns.size(), index -> columns.get(index).getName());
    }

    private static RowLayout materialize(int size, IntFunction<String> columnName) {
        if (size <= LINEAR_LOOKUP_LIMIT) {
            return materializeSmall(size, columnName);
        }
        return materializeIndexed(size, columnName);
    }

    private static RowLayout materializeSmall(int size, IntFunction<String> columnName) {
        String[] names = new String[size];
        String[] normalized = new String[size];
        boolean ambiguous = false;
        for (int index = 0; index < size; index++) {
            String name = requireColumnName(columnName.apply(index));
            for (int previous = 0; previous < index; previous++) {
                if (names[previous].equals(name)) {
                    throw new DuplicateColumnLabelException(name, previous, index);
                }
            }
            String mappingKey = EntityFieldNames.resultKey(name);
            if (!ambiguous) {
                for (int previous = 0; previous < index; previous++) {
                    if (normalized[previous].equals(mappingKey)) {
                        ambiguous = true;
                        break;
                    }
                }
            }
            names[index] = name;
            normalized[index] = mappingKey;
        }
        return new RowLayout(names, Map.of(), normalized, Map.of(), ambiguous);
    }

    private static RowLayout materializeIndexed(int size, IntFunction<String> columnName) {
        String[] names = new String[size];
        Map<String, Integer> indexes = new HashMap<>(size * 2);
        String[] normalized = new String[size];
        Map<String, Integer> mappingIndexes = new HashMap<>(size * 2);
        boolean ambiguous = false;
        for (int index = 0; index < size; index++) {
            String name = requireColumnName(columnName.apply(index));
            Integer previous = indexes.putIfAbsent(name, index);
            if (previous != null) {
                throw new DuplicateColumnLabelException(name, previous, index);
            }
            names[index] = name;
            String mappingKey = EntityFieldNames.resultKey(name);
            ambiguous |= mappingIndexes.putIfAbsent(mappingKey, index) != null;
            normalized[index] = mappingKey;
        }
        return new RowLayout(
                names, Map.copyOf(indexes), normalized, Map.copyOf(mappingIndexes), ambiguous);
    }

    int size() {
        return columnNames.length;
    }

    String columnName(int index) {
        Objects.checkIndex(index, columnNames.length);
        return columnNames[index];
    }

    int indexOf(Object column) {
        if (!(column instanceof String name)) {
            return -1;
        }
        if (!indexes.isEmpty()) {
            return indexes.getOrDefault(name, -1);
        }
        for (int index = 0; index < columnNames.length; index++) {
            if (columnNames[index].equals(name)) {
                return index;
            }
        }
        return -1;
    }

    String mappingKey(int index) {
        Objects.checkIndex(index, mappingKeys.length);
        return mappingKeys[index];
    }

    int mappingIndexOf(String key) {
        if (!mappingIndexes.isEmpty()) {
            return mappingIndexes.getOrDefault(key, -1);
        }
        for (int index = 0; index < mappingKeys.length; index++) {
            if (mappingKeys[index].equals(key)) {
                return index;
            }
        }
        return -1;
    }

    boolean hasAmbiguousMappingColumns() {
        return ambiguousMappingColumns;
    }

    @SuppressWarnings("unchecked")
    synchronized <T> T mappingBinding(Object plan, Supplier<? extends T> factory) {
        Object safePlan = Objects.requireNonNull(plan, "mapping plan must not be null");
        Supplier<? extends T> safeFactory = Objects.requireNonNull(
                factory, "mapping binding factory must not be null");
        if (mappingBindings != null && mappingBindings.containsKey(safePlan)) {
            return (T) mappingBindings.get(safePlan);
        }
        T binding = Objects.requireNonNull(safeFactory.get(), "mapping binding must not be null");
        if (mappingBindings == null) {
            mappingBindings = new IdentityHashMap<>();
        }
        mappingBindings.put(safePlan, binding);
        return binding;
    }

    private static String requireColumnName(String name) {
        String safeName = Objects.requireNonNull(name, "row column label must not be null");
        if (safeName.isBlank()) {
            throw new IllegalArgumentException("row column label must not be blank");
        }
        return safeName;
    }
}
