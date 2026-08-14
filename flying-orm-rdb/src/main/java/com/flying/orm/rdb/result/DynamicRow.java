package com.flying.orm.rdb.result;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;

/**
 * 动态查询的一行紧凑结果。
 *
 * <p>它仍然是大家熟悉的只读 {@code Map<String, Object>}，但内部没有逐列 Map 节点：同一结果集共享
 * 一份列布局，每行只保存一个值数组。按列名、按下标和遍历都不会复制整行；只有显式调用
 * {@link #toMap()} 时才真正创建普通 Map。</p>
 *
 * <p>实例发布后不可修改，可以安全地在线程和响应式边界间传递。数据库驱动值本身如果是可变对象，
 * 仍由对应 codec 或使用方按该类型的约定处理。</p>
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class DynamicRow extends AbstractMap<String, Object> {

    private final RowLayout layout;

    private final Object[] values;

    private DynamicRow(RowLayout layout, Object[] ownedValues) {
        this.layout = Objects.requireNonNull(layout, "row layout must not be null");
        this.values = Objects.requireNonNull(ownedValues, "row values must not be null");
        if (layout.size() != ownedValues.length) {
            throw new IllegalArgumentException("row value count does not match column count: columns="
                    + layout.size() + ", values=" + ownedValues.length);
        }
    }

    /**
     * 接管一个刚创建且不会再被修改的值数组。只给结果读取和解码链路使用，避免无意义的第二次数组复制。
     */
    static DynamicRow owned(RowLayout layout, Object[] ownedValues) {
        return new DynamicRow(layout, ownedValues);
    }

    /**
     * 把外部 Map 安全地压缩成一行动态结果。
     *
     * <p>这个入口主要给自定义执行器、测试替身和适配层使用。方法会按 Map 的迭代顺序复制列名和值，
     * 后续再修改原 Map 不会影响已经发布的结果。数据库查询主链路不会走这里，它会在一个结果集内
     * 共享 {@link RowLayout}，从而省掉每行重复保存列名的开销。</p>
     */
    public static DynamicRow copyOf(Map<String, ?> source) {
        Map<String, ?> safeSource = Objects.requireNonNull(source, "source row must not be null");
        String[] names = new String[safeSource.size()];
        Object[] copiedValues = new Object[safeSource.size()];
        int index = 0;
        for (Entry<String, ?> entry : safeSource.entrySet()) {
            names[index] = entry.getKey();
            copiedValues[index] = entry.getValue();
            index++;
        }
        return owned(RowLayout.of(java.util.Arrays.asList(names)), copiedValues);
    }

    RowLayout layout() {
        return layout;
    }

    public int columnCount() {
        return values.length;
    }

    public String columnName(int index) {
        return layout.columnName(index);
    }

    public Object value(int index) {
        Objects.checkIndex(index, values.length);
        return values[index];
    }

    public <T> T get(String column, Class<T> type) {
        Objects.requireNonNull(type, "row value type must not be null");
        Object value = get(column);
        return value == null ? null : type.cast(value);
    }

    /**
     * 一次替换若干已经解码的列值，并继续复用原来的列布局。
     *
     * <p>动态表单的 JSON、数组和大字段可能需要异步解码。调用方先收集真正发生变化的列下标，
     * 最后通过本方法只复制一次值数组；原行保持不变，空替换则直接返回当前实例。</p>
     */
    public DynamicRow withValues(Map<Integer, ?> replacements) {
        Map<Integer, ?> safeReplacements = Objects.requireNonNull(replacements,
                                                                   "row value replacements must not be null");
        if (safeReplacements.isEmpty()) {
            return this;
        }
        Object[] copied = values.clone();
        safeReplacements.forEach((index, value) -> copied[Objects.checkIndex(index, copied.length)] = value);
        return owned(layout, copied);
    }

    /**
     * 只替换列标签并继续共享当前值数组，供 SQL 别名映射使用。
     *
     * <p>重命名后的布局仍会检查空名称和重复名称。值数组从发布后就不会再修改，因此两个只读行共享它
     * 不会产生并发问题，也比先复制成 LinkedHashMap 再映射少很多短命对象。</p>
     */
    public DynamicRow renameColumns(UnaryOperator<String> renamer) {
        UnaryOperator<String> safeRenamer = Objects.requireNonNull(renamer, "column renamer must not be null");
        java.util.ArrayList<String> names = new java.util.ArrayList<>(values.length);
        for (int index = 0; index < values.length; index++) {
            names.add(safeRenamer.apply(layout.columnName(index)));
        }
        return owned(RowLayout.of(names), values);
    }

    @Override
    public Object get(Object key) {
        int index = layout.indexOf(key);
        return index < 0 ? null : values[index];
    }

    @Override
    public boolean containsKey(Object key) {
        return layout.indexOf(key) >= 0;
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public void forEach(BiConsumer<? super String, ? super Object> action) {
        Objects.requireNonNull(action, "row action must not be null");
        for (int index = 0; index < values.length; index++) {
            action.accept(layout.columnName(index), values[index]);
        }
    }

    /**
     * 显式物化成一份独立、只读的 LinkedHashMap。普通读取不要调用这个方法。
     */
    public Map<String, Object> toMap() {
        Map<String, Object> materialized = new LinkedHashMap<>(capacity(values.length));
        forEach(materialized::put);
        return Collections.unmodifiableMap(materialized);
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return new AbstractSet<>() {
            @Override
            public Iterator<Entry<String, Object>> iterator() {
                return new Iterator<>() {
                    private int index;

                    @Override
                    public boolean hasNext() {
                        return index < values.length;
                    }

                    @Override
                    public Entry<String, Object> next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        int current = index++;
                        return new SimpleImmutableEntry<>(layout.columnName(current), values[current]);
                    }
                };
            }

            @Override
            public int size() {
                return values.length;
            }
        };
    }

    private static int capacity(int size) {
        return size < 3 ? size + 1 : (int) Math.ceil(size / 0.75d);
    }
}
