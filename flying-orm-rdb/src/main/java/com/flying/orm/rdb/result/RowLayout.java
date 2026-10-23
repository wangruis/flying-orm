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
import java.util.function.UnaryOperator;

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

    /** 实体映射首次使用时才准备；最后发布 keys，让其他线程同时看到完整索引。 */
    private volatile String[] mappingKeys;

    private Map<String, Integer> mappingIndexes;

    private boolean ambiguousMappingColumns;

    /** 按计划身份发布只读快照；逐行复用不加锁，生命周期不会超过当前 RowLayout。 */
    private volatile Map<Object, Object> mappingBindings = Map.of();

    private RowLayout(String[] columnNames, Map<String, Integer> indexes) {
        this.columnNames = columnNames;
        this.indexes = indexes;
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
        for (int index = 0; index < size; index++) {
            String name = requireColumnName(columnName.apply(index));
            for (int previous = 0; previous < index; previous++) {
                if (names[previous].equals(name)) {
                    throw new DuplicateColumnLabelException(name, previous, index);
                }
            }
            names[index] = name;
        }
        return new RowLayout(names, Map.of());
    }

    private static RowLayout materializeIndexed(int size, IntFunction<String> columnName) {
        String[] names = new String[size];
        Map<String, Integer> indexes = new HashMap<>(size * 2);
        for (int index = 0; index < size; index++) {
            String name = requireColumnName(columnName.apply(index));
            Integer previous = indexes.putIfAbsent(name, index);
            if (previous != null) {
                throw new DuplicateColumnLabelException(name, previous, index);
            }
            names[index] = name;
        }
        return new RowLayout(names, indexes);
    }

    int size() {
        return columnNames.length;
    }

    String columnName(int index) {
        Objects.checkIndex(index, columnNames.length);
        return columnNames[index];
    }

    RowLayout rename(UnaryOperator<String> renamer) {
        String[] renamed = null;
        for (int index = 0; index < columnNames.length; index++) {
            String name = renamer.apply(columnNames[index]);
            if (!columnNames[index].equals(name)) {
                if (renamed == null) renamed = columnNames.clone();
                renamed[index] = name;
            }
        }
        return renamed == null ? this : RowLayout.of(java.util.Arrays.asList(renamed));
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
        if (mappingKeys == null) prepareMapping();
        Objects.checkIndex(index, mappingKeys.length);
        return mappingKeys[index];
    }

    int mappingIndexOf(String key) {
        if (mappingKeys == null) prepareMapping();
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
        if (mappingKeys == null) prepareMapping();
        return ambiguousMappingColumns;
    }

    private synchronized void prepareMapping() {
        if (mappingKeys != null) return;
        String[] normalized = new String[columnNames.length];
        Map<String, Integer> resolved = columnNames.length > LINEAR_LOOKUP_LIMIT
                ? new HashMap<>(columnNames.length * 2) : null;
        boolean ambiguous = false;
        for (int index = 0; index < columnNames.length; index++) {
            String key = EntityFieldNames.resultKey(columnNames[index]);
            if (resolved != null) {
                ambiguous |= resolved.putIfAbsent(key, index) != null;
            } else if (!ambiguous) {
                for (int previous = 0; previous < index; previous++) {
                    if (normalized[previous].equals(key)) {
                        ambiguous = true;
                        break;
                    }
                }
            }
            normalized[index] = key;
        }
        mappingIndexes = resolved == null ? Map.of() : resolved;
        ambiguousMappingColumns = ambiguous;
        mappingKeys = normalized;
    }

    @SuppressWarnings("unchecked")
    <T> T mappingBinding(Object plan, Supplier<? extends T> factory) {
        Object safePlan = Objects.requireNonNull(plan, "mapping plan must not be null");
        Supplier<? extends T> safeFactory = Objects.requireNonNull(
                factory, "mapping binding factory must not be null");
        Object binding = mappingBindings.get(safePlan);
        if (binding != null) {
            return (T) binding;
        }
        synchronized (this) {
            binding = mappingBindings.get(safePlan);
            if (binding == null) {
                binding = Objects.requireNonNull(safeFactory.get(), "mapping binding must not be null");
                Map<Object, Object> resolved = new IdentityHashMap<>(mappingBindings);
                resolved.put(safePlan, binding);
                mappingBindings = resolved;
            }
            return (T) binding;
        }
    }

    private static String requireColumnName(String name) {
        String safeName = Objects.requireNonNull(name, "row column label must not be null");
        if (safeName.isBlank()) {
            throw new IllegalArgumentException("row column label must not be blank");
        }
        return safeName;
    }
}
