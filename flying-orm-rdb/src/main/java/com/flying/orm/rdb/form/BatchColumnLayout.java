package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 固定一次批量写入的列布局，把字段规范名提前映射成参数下标。首行确定 SQL 列顺序，后续每行即使 Map
 * 迭代顺序不同，也会按同一位置输出参数，避免列和值错位。
 *
 * <p>布局同时负责逐字段 codec 转换，确保首行和后续行遵循完全相同的 JSON、Array、LOB 和时间规则。
 * record 内部集合在构造时复制，可在同一次批量流水线中安全读取。</p>
 *
 * @author wangr
 * @date 2026-07-24
 * @version v1.0
 */
record BatchColumnLayout(DynamicForm form,
                         List<DynamicField> fields,
                         Map<String, Integer> indexesByNormalizedName,
                         FormSqlRenderSupport support) {

    BatchColumnLayout {
        form = Objects.requireNonNull(form, "dynamic form must not be null");
        fields = List.copyOf(Objects.requireNonNull(fields, "batch column fields must not be null"));
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("batch column fields must not be empty");
        }
        indexesByNormalizedName = Map.copyOf(Objects.requireNonNull(indexesByNormalizedName,
                                                                     "batch column indexes must not be null"));
        support = Objects.requireNonNull(support, "form SQL render support must not be null");
        // 一列必须恰好对应一个参数位置；数量不一致说明布局构造存在重复或遗漏。
        if (indexesByNormalizedName.size() != fields.size()) {
            throw new IllegalArgumentException("batch column indexes must match field count");
        }
    }

    static BatchColumnLayout of(DynamicForm form,
                                List<DynamicField> fields,
                                FormSqlRenderSupport support) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        List<DynamicField> safeFields = List.copyOf(Objects.requireNonNull(fields, "batch column fields must not be null"));
        Map<String, Integer> indexes = new HashMap<>(Math.max(16, safeFields.size() * 2));
        // form.field 同时确认列属于当前动态表单，不能把别的表字段混入这一批 SQL。
        for (int i = 0; i < safeFields.size(); i++) {
            DynamicField field = Objects.requireNonNull(safeFields.get(i), "batch column field must not be null");
            String normalizedName = field.normalizedName();
            Integer previous = indexes.putIfAbsent(normalizedName, i);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate batch column field: " + field.name());
            }
            safeForm.field(field.name());
        }
        return new BatchColumnLayout(safeForm,
                                     safeFields,
                                     indexes,
                                     support);
    }

    Object[] parameters(Map<String, Object> row, long rowIndex) {
        Map<String, Object> safeRow = Objects.requireNonNull(row, "batch insert row must not be null");
        if (safeRow.size() != fields.size()) {
            throw new IllegalArgumentException("batch insert row [" + rowIndex + "] fields must match the first row");
        }

        // seen 数组避免每行构建临时 Set，同时能发现规范化后重复的字段名。
        Object[] parameters = new Object[fields.size()];
        boolean[] seen = new boolean[fields.size()];
        int seenCount = 0;
        for (Map.Entry<String, Object> entry : safeRow.entrySet()) {
            DynamicField field = form.field(entry.getKey());
            Integer index = indexesByNormalizedName.get(field.normalizedName());
            if (index == null) {
                throw new IllegalArgumentException("batch insert row [" + rowIndex + "] fields must match the first row");
            }
            if (seen[index]) {
                throw new IllegalArgumentException("duplicate normalized batch insert field");
            }
            seen[index] = true;
            seenCount++;
            DynamicField layoutField = fields.get(index);
            Object value = entry.getValue();
            if (value instanceof UpdateDelta) {
                throw new IllegalArgumentException("batch write row [" + rowIndex + "] field ["
                                                           + layoutField.name() + "] does not allow update delta");
            }
            parameters[index] = write(layoutField, value);
        }
        if (seenCount != fields.size()) {
            throw new IllegalArgumentException("batch insert row [" + rowIndex + "] fields must match the first row");
        }
        return parameters;
    }

    /**
     * 首行和后续行必须走同一套字段转换，否则批量第二行才会暴露类型错误。
     */
    private Object write(DynamicField field, Object value) {
        return support.writeValue(field, value);
    }
}
