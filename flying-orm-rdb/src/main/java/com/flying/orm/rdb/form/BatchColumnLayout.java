package com.flying.orm.rdb.form;

import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.expression.ValueExpression;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 固定一次批量写入的列布局，按需建立字段规范名到参数下标的索引。首行确定 SQL 列顺序，后续每行即使 Map
 * 迭代顺序不同，也会按同一位置输出参数，避免列和值错位。
 *
 * <p>布局同时负责逐字段 codec 转换，确保首行和后续行遵循完全相同的 JSON、Array、LOB 和时间规则。
 * 字段列表在构造时冻结，索引由布局独占且不对外暴露，可在同一次批量流水线中安全读取。</p>
 *
 * @author wangr
 * @date 2026-07-24
 * @version v1.0
 */
final class BatchColumnLayout {

    private static final Object UNBOUND = new Object();

    private final DynamicForm form;
    private final List<DynamicField> fields;
    private volatile Map<String, Integer> indexesByNormalizedName;
    private final FormSqlRenderSupport support;
    private final List<String> expressions;
    private final int[] offsets;

    private BatchColumnLayout(DynamicForm form, FormSqlRenderSupport.WriteFields row, FormSqlRenderSupport support) {
        this.form = Objects.requireNonNull(form, "dynamic form must not be null");
        this.fields = List.copyOf(row.fields());
        this.support = Objects.requireNonNull(support, "form SQL render support must not be null");
        this.expressions = row.expressions();
        this.offsets = row.offsets();
    }

    /** 与首行布局同序的行直接绑定；重排行才需要名称索引。 */
    private Map<String, Integer> indexes() {
        Map<String, Integer> current = indexesByNormalizedName;
        if (current != null) return current;
        current = HashMap.newHashMap(fields.size());
        // 完整构建后一次发布；并发首次使用即使重复构建，也不共享正在修改的 Map。
        for (int index = 0; index < this.fields.size(); index++) {
            DynamicField field = this.fields.get(index);
            Integer previous = current.putIfAbsent(field.normalizedName(), index);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate batch column field: " + field.name());
            }
            form.field(field.name());
        }
        indexesByNormalizedName = current;
        return current;
    }

    static BatchColumnLayout of(DynamicForm form,
                                FormSqlRenderSupport.WriteFields row,
                                FormSqlRenderSupport support) {
        return new BatchColumnLayout(form, row, support);
    }

    List<DynamicField> fields() {
        return fields;
    }

    Integer indexOf(String normalizedName) {
        return indexes().get(normalizedName);
    }

    boolean hasExpressions() { return offsets != null; }

    int parameterIndex(int column) { return offsets == null ? column : offsets[column]; }

    int parameterCount() { return parameterIndex(fields.size()); }

    boolean computed(int column) {
        return hasExpressions() && !expressions.get(column).equals(support.valueExpression(fields.get(column)));
    }

    Object[] parameters(Map<String, Object> row, long rowIndex) {
        Object[] parameters = new Object[parameterCount()];
        writeParameters(row, rowIndex, parameters);
        return parameters;
    }

    /** 调用方拥有目标数组；先填业务参数，再按既定 SQL 布局插入批次共用的 Scope 参数。 */
    void writeParameters(Map<String, Object> row, long rowIndex, Object[] parameters) {
        Map<String, Object> safeRow = Objects.requireNonNull(row, "batch insert row must not be null");
        if (safeRow.size() != fields.size()) {
            throw new IllegalArgumentException("batch insert row [" + rowIndex + "] fields must match the first row");
        }
        if (hasExpressions()) {
            FormSqlRenderSupport.WriteFields encoded = support.writeFields(form, safeRow, rowIndex);
            if (!encoded.fields().containsAll(fields)) {
                throw new IllegalArgumentException("batch insert row [" + rowIndex + "] fields must match the first row");
            }
            encoded = encoded.select(fields);
            if (!expressions.equals(encoded.expressions(support)) || encoded.parameters().length != parameterCount()) {
                throw new IllegalArgumentException("batch insert expressions must match the current SQL layout");
            }
            System.arraycopy(encoded.parameters(), 0, parameters, 0, parameterCount());
            return;
        }

        boolean ordered = true;
        int seenCount = 0;
        for (Map.Entry<String, Object> entry : safeRow.entrySet()) {
            int index = seenCount;
            DynamicField expected = fields.get(index);
            String name = expected.name().equals(entry.getKey())
                    ? expected.normalizedName() : FieldIdentity.of(entry.getKey()).key();
            if (!expected.normalizedName().equals(name)) {
                // 首次乱序才标记未绑定后缀；已绑定前缀（包括 null）保留，Scope 尾部不受影响。
                if (ordered) {
                    Arrays.fill(parameters, seenCount, fields.size(), UNBOUND);
                    ordered = false;
                }
                Integer mappedIndex = indexes().get(name);
                if (mappedIndex == null) {
                    form.field(entry.getKey()); // 保留未知字段与首行布局不匹配的异常区别。
                    throw new IllegalArgumentException("batch insert row [" + rowIndex + "] fields must match the first row");
                }
                index = mappedIndex;
            }
            if (!ordered && parameters[index] != UNBOUND) {
                throw new IllegalArgumentException("duplicate normalized batch insert field");
            }
            seenCount++;
            DynamicField layoutField = fields.get(index);
            Object value = entry.getValue();
            if (value instanceof UpdateDelta) {
                throw new IllegalArgumentException("batch write row [" + rowIndex + "] field ["
                                                           + layoutField.name() + "] does not allow update delta");
            }
            if (value instanceof ValueExpression.Value bound) value = bound.value();
            parameters[index] = support.writeValue(layoutField, value);
        }
        if (seenCount != fields.size()) {
            throw new IllegalArgumentException("batch insert row [" + rowIndex + "] fields must match the first row");
        }
    }

}
