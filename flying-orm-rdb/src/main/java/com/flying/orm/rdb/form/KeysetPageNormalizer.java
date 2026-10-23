package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.page.CursorDirection;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetSort;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * 把调用方 keyset 请求规范成包含可靠关系键的稳定排序。
 *
 * <p>优先使用完整主键。没有主键时，只接受全部列 NOT NULL 且已确认具有普通全局唯一语义的约束；
 * nullable、条件、表达式或能力未知的唯一性都不能在这里“猜成” tie-breaker。整个过程只看元数据，
 * 发生拒绝时还没有获取连接。</p>
 *
 * @author wangr
 * @version v3.2
 */
final class KeysetPageNormalizer {

    private KeysetPageNormalizer() {
    }

    static NormalizedKeysetPage normalize(DynamicForm form, KeysetPageQuery page) {
        DynamicForm safeForm = Objects.requireNonNull(form, "keyset form must not be null");
        return normalize(page,
                         field -> safeForm.field(field).name(),
                         field -> safeForm.field(field).nullable(),
                         dynamicStableKey(safeForm));
    }

    /**
     * 规范关系元数据路径。只有方言已经认证普通 UNIQUE 的全局语义时，才允许把非空唯一约束当后备键。
     */
    static NormalizedKeysetPage normalize(RelationalTableDefinition table,
                                           KeysetPageQuery page,
                                           boolean certifiedGlobalUniqueConstraints) {
        RelationalTableDefinition safeTable = Objects.requireNonNull(
                table, "keyset relational table must not be null");
        List<String> stableKey = safeTable.primaryKey()
                .map(primaryKey -> primaryKey.columns())
                .orElseGet(() -> certifiedGlobalUniqueConstraints
                        ? firstNonNullUnique(safeTable)
                        : List.of());
        return normalize(page,
                         field -> safeTable.column(field).name(),
                         field -> safeTable.column(field).nullable(),
                         stableKey);
    }

    static NormalizedKeysetPage normalize(KeysetPageQuery page,
                                           Function<String, String> fields,
                                           Predicate<String> nullableFields,
                                           List<String> stableKey) {
        KeysetPageQuery safePage = Objects.requireNonNull(page, "keyset page must not be null");
        if (stableKey.isEmpty()) {
            throw new IllegalArgumentException(
                    "keyset pagination requires a non-null primary key or certified unique key");
        }

        Map<String, KeysetSort> normalized = new LinkedHashMap<>();
        for (KeysetSort sort : safePage.sorts()) {
            String field = fields.apply(sort.field());
            KeysetSort canonical = new KeysetSort(field, sort.direction(), sort.nullOrder());
            KeysetSort previous = normalized.putIfAbsent(canonical.field(), canonical);
            if (previous != null && !previous.equals(canonical)) {
                throw new IllegalArgumentException("keyset sort field has conflicting definitions");
            }
        }
        int callerSortCount = normalized.size();

        KeysetSort lastCallerSort = normalized.values().stream()
                .reduce((left, right) -> right)
                .orElseThrow(() -> new IllegalArgumentException(
                        "keyset pagination requires at least one sort field"));
        for (String stableField : stableKey) {
            String field = fields.apply(stableField);
            if (!normalized.containsKey(com.flying.orm.core.field.FieldIdentity.of(field).key())) {
                KeysetSort appended = new KeysetSort(
                        field, lastCallerSort.direction(), lastCallerSort.nullOrder());
                normalized.put(appended.field(), appended);
            }
        }

        List<KeysetSort> sorts = List.copyOf(normalized.values());
        // CursorPosition 的公开 accessor 为保护调用方会深快照；越过公开边界后只取一次并由本次计划独占。
        List<Object> positionValues = safePage.firstPage()
                ? List.of() : safePage.position().values();
        if (!safePage.firstPage() && positionValues.size() != sorts.size()) {
            throw new IllegalArgumentException(
                    "keyset cursor position must match the final stable sort");
        }
        List<Boolean> nullableSorts = sorts.stream()
                .map(sort -> nullableFields.test(sort.field()))
                .toList();
        NormalizedKeysetPage normalizedPage = new NormalizedKeysetPage(
                safePage.size(), sorts, positionValues, nullableSorts,
                callerSortCount);
        requireValidNullPositions(normalizedPage);
        return normalizedPage;
    }

    static List<String> dynamicStableKey(DynamicForm form) {
        List<String> primaryKey = form.fields().stream()
                .filter(DynamicField::primaryKey)
                .map(DynamicField::name)
                .toList();
        if (!primaryKey.isEmpty()) {
            return primaryKey;
        }
        return form.fields().stream()
                .filter(DynamicField::unique)
                .filter(field -> !field.nullable())
                .map(field -> List.of(field.name()))
                .findFirst()
                .orElseGet(List::of);
    }

    private static List<String> firstNonNullUnique(RelationalTableDefinition table) {
        for (UniqueConstraintDefinition unique : table.uniqueConstraints()) {
            boolean allNonNull = unique.columns().stream()
                    .map(table::column)
                    .noneMatch(column -> column.nullable());
            if (allNonNull) {
                return unique.columns();
            }
        }
        return List.of();
    }

    private static void requireValidNullPositions(NormalizedKeysetPage page) {
        if (page.firstPage()) {
            return;
        }
        List<Object> values = page.positionValues();
        for (int index = 0; index < page.sorts().size(); index++) {
            if (values.get(index) == null && !page.nullable(index)) {
                throw new IllegalArgumentException(
                        "non-null keyset sort field must not have a null cursor position");
            }
        }
    }

    /** 规范化器拥有这些已冻结的列表；执行端只读取本次调用的排序与位置。 */
    record NormalizedKeysetPage(int size,
                                List<KeysetSort> sorts,
                                List<Object> positionValues,
                                List<Boolean> nullableSorts,
                                int callerSortCount) {
        public boolean firstPage() { return positionValues().isEmpty(); }
        public int sortCount() { return sorts.size(); }
        public String field(int index) { return sorts.get(index).field(); }
        public CursorDirection direction(int index) { return sorts.get(index).direction(); }
        public NullOrder nullOrder(int index) { return sorts.get(index).nullOrder(); }
        public boolean nullable(int index) { return nullableSorts.get(index); }
    }
}
