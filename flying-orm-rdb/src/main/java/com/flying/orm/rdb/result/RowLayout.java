package com.flying.orm.rdb.result;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    private RowLayout(String[] columnNames, Map<String, Integer> indexes) {
        this.columnNames = columnNames;
        this.indexes = indexes;
    }

    static RowLayout of(List<String> columnNames) {
        List<String> safeNames = List.copyOf(Objects.requireNonNull(
                columnNames, "row column names must not be null"));
        String[] names = new String[safeNames.size()];
        Map<String, Integer> discovered = new HashMap<>(Math.max(16, safeNames.size() * 2));
        for (int index = 0; index < safeNames.size(); index++) {
            String name = requireColumnName(safeNames.get(index));
            Integer previous = discovered.putIfAbsent(name, index);
            if (previous != null) {
                throw new DuplicateColumnLabelException(name, previous, index);
            }
            names[index] = name;
        }
        Map<String, Integer> retained = names.length <= LINEAR_LOOKUP_LIMIT
                ? Map.of()
                : Map.copyOf(discovered);
        return new RowLayout(names, retained);
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

    private static String requireColumnName(String name) {
        String safeName = Objects.requireNonNull(name, "row column label must not be null");
        if (safeName.isBlank()) {
            throw new IllegalArgumentException("row column label must not be blank");
        }
        return safeName;
    }
}
