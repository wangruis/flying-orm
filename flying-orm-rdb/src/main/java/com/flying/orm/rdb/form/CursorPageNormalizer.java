package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorDirection;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorSort;
import com.flying.orm.rdb.codec.LargeObjectValueCodec;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 将调用方游标排序规范化为以实体主键保证唯一性的稳定排序。
 *
 * <p>该组件由 SQL 渲染和下一游标生成共同使用：先校验显式排序字段存在且不可空，再按实体字段顺序追加缺失
 * 主键。追加方向沿用最后一个显式排序方向。相同字段的同向重复项会合并，方向冲突则 fail-closed；没有主键、
 * 可空排序或非首屏游标槽数不匹配都会在 SQL 执行前失败。</p>
 *
 * @author wangr
 * @date 2026-08-04
 * @version v1.0
 */
final class CursorPageNormalizer {

    private CursorPageNormalizer() {
    }

    /**
     * 返回与实体元数据一致的稳定游标请求。
     *
     * @param form 当前经过读取字段范围裁剪的表单
     * @param page 调用方游标请求
     * @return 排序已去重并补齐主键的新请求
     */
    static NormalizedCursorPage normalize(DynamicForm form, CursorPageQuery page) {
        DynamicForm safeForm = Objects.requireNonNull(form, "cursor form must not be null");
        CursorPageQuery safePage = Objects.requireNonNull(page, "cursor page query must not be null");
        List<DynamicField> primaryKeys = safeForm.fields().stream().filter(DynamicField::primaryKey).toList();
        if (primaryKeys.isEmpty()) {
            throw new IllegalArgumentException("cursor pagination requires at least one primary-key field");
        }

        Map<String, CursorSort> uniqueSorts = new LinkedHashMap<>();
        for (CursorSort sort : safePage.sorts()) {
            DynamicField field = requireSortable(safeForm, sort.field());
            String normalizedName = field.identity().key();
            CursorSort canonical = new CursorSort(field.name(), sort.direction());
            CursorSort previous = uniqueSorts.putIfAbsent(normalizedName, canonical);
            if (previous != null && previous.direction() != canonical.direction()) {
                throw new IllegalArgumentException("cursor sort field has conflicting directions: " + field.name());
            }
        }
        int callerSortCount = uniqueSorts.size();

        CursorDirection appendedDirection = uniqueSorts.values().stream()
                .reduce((first, second) -> second)
                .orElseThrow(() -> new IllegalArgumentException("cursor pagination requires at least one sort field"))
                .direction();
        for (DynamicField primaryKey : primaryKeys) {
            uniqueSorts.putIfAbsent(primaryKey.identity().key(),
                                    new CursorSort(primaryKey.name(), appendedDirection));
        }

        List<CursorSort> normalizedSorts = List.copyOf(new ArrayList<>(uniqueSorts.values()));
        List<Object> cursor = safePage.cursor();
        if (!safePage.firstPage() && cursor.size() != normalizedSorts.size()) {
            throw new IllegalArgumentException("cursor value count must match normalized sort field count: expected "
                                                       + normalizedSorts.size() + " but was " + cursor.size());
        }
        return new NormalizedCursorPage(safePage.size(), normalizedSorts, cursor, callerSortCount);
    }

    private static DynamicField requireSortable(DynamicForm form, String fieldName) {
        DynamicField field = form.findField(fieldName)
                                 .orElseThrow(() -> new IllegalArgumentException(
                                         "cursor sort field does not exist"));
        if (field.nullable()) {
            throw new IllegalArgumentException("cursor sort field must not be nullable: " + field.name());
        }
        if (field.databaseType().logicalType() == LogicalType.JSON
                || field.databaseType().logicalType() == LogicalType.VECTOR
                || LargeObjectValueCodec.isLargeObjectDataType(field.databaseType())) {
            throw new IllegalArgumentException(
                    "cursor ordering does not support this field type: " + field.name());
        }
        return field;
    }

    /** 包内规范化结果独占公共 accessor 产生的游标快照，且只能由本规范化器创建。 */
    static final class NormalizedCursorPage {

        private final int size;
        private final List<CursorSort> sorts;
        private final List<Object> cursor;
        private final int callerSortCount;

        private NormalizedCursorPage(int size,
                                     List<CursorSort> sorts,
                                     List<Object> cursor,
                                     int callerSortCount) {
            this.size = size;
            this.sorts = sorts;
            this.cursor = cursor;
            this.callerSortCount = callerSortCount;
        }

        int size() {
            return size;
        }

        List<CursorSort> sorts() {
            return sorts;
        }

        List<Object> cursor() {
            return cursor;
        }

        boolean firstPage() {
            return cursor.isEmpty();
        }

        int callerSortCount() {
            return callerSortCount;
        }
    }
}
