package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetSort;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    private static NormalizedKeysetPage normalize(KeysetPageQuery page,
                                                   FieldResolver fields,
                                                   NullableResolver nullableFields,
                                                   List<String> stableKey) {
        KeysetPageQuery safePage = Objects.requireNonNull(page, "keyset page must not be null");
        if (stableKey.isEmpty()) {
            throw new IllegalArgumentException(
                    "keyset pagination requires a non-null primary key or certified unique key");
        }

        Map<String, KeysetSort> normalized = new LinkedHashMap<>();
        for (KeysetSort sort : safePage.sorts()) {
            String field = fields.resolve(sort.field());
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
        List<String> hiddenTieBreakers = new ArrayList<>(stableKey.size());
        for (String stableField : stableKey) {
            String field = fields.resolve(stableField);
            if (!normalized.containsKey(com.flying.orm.core.field.FieldIdentity.of(field).key())) {
                KeysetSort appended = new KeysetSort(
                        field, lastCallerSort.direction(), lastCallerSort.nullOrder());
                normalized.put(appended.field(), appended);
                hiddenTieBreakers.add(appended.field());
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
                .map(sort -> nullableFields.nullable(sort.field()))
                .toList();
        NormalizedKeysetPage normalizedPage = new NormalizedKeysetPage(
                safePage.size(), sorts, positionValues, hiddenTieBreakers, nullableSorts,
                callerSortCount);
        requireValidNullPositions(normalizedPage);
        return normalizedPage;
    }

    private static List<String> dynamicStableKey(DynamicForm form) {
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

    @FunctionalInterface
    private interface FieldResolver {
        String resolve(String field);
    }

    @FunctionalInterface
    private interface NullableResolver {
        boolean nullable(String field);
    }

    /** 一次调用内使用的只读规范化结果，不进入跨调用的动态权限缓存。 */
    static final class NormalizedKeysetPage {

        private final int size;
        private final List<KeysetSort> sorts;
        private final List<Object> positionValues;
        private final List<String> hiddenTieBreakers;
        private final List<Boolean> nullableSorts;
        private final int callerSortCount;

        private NormalizedKeysetPage(int size,
                                     List<KeysetSort> sorts,
                                     List<Object> positionValues,
                                     List<String> hiddenTieBreakers,
                                     List<Boolean> nullableSorts,
                                     int callerSortCount) {
            this.size = size;
            this.sorts = List.copyOf(sorts);
            this.positionValues = Objects.requireNonNull(
                    positionValues, "keyset position values must not be null");
            this.hiddenTieBreakers = List.copyOf(hiddenTieBreakers);
            this.nullableSorts = List.copyOf(nullableSorts);
            this.callerSortCount = callerSortCount;
            if (this.nullableSorts.size() != this.sorts.size()) {
                throw new IllegalArgumentException("keyset nullability must match stable sorts");
            }
            if (callerSortCount < 1 || callerSortCount > this.sorts.size()) {
                throw new IllegalArgumentException("keyset caller sort count must match stable sorts");
            }
        }

        int size() {
            return size;
        }

        List<KeysetSort> sorts() {
            return sorts;
        }

        List<Object> positionValues() {
            return positionValues;
        }

        List<String> hiddenTieBreakers() {
            return hiddenTieBreakers;
        }

        boolean firstPage() {
            return positionValues.isEmpty();
        }

        boolean nullable(int index) {
            return nullableSorts.get(index);
        }

        int callerSortCount() {
            return callerSortCount;
        }
    }
}
