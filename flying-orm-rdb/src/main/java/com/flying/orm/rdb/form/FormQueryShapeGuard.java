package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.rdb.form.spec.QuerySpec;

import java.util.List;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * 在 SQL 计划生成前统一校验投影、分组和排序的只读形状。
 *
 * <p>这里只解释可信 {@link DynamicForm} 中的字段关系，不生成 SQL、不访问数据库，也不保存请求状态。</p>
 *
 * @author wangr
 * @date 2026-08-16
 * @version v2.0
 */
final class FormQueryShapeGuard {

    private FormQueryShapeGuard() {
    }

    static void requireContainsShape(QuerySpec spec) {
        if (!spec.groups().isEmpty()) {
            throw new IllegalArgumentException("protected contains search does not support grouped queries");
        }
    }

    static void requireUngroupedPagination(QuerySpec spec, String pagination) {
        if (!spec.groups().isEmpty()) {
            throw new IllegalArgumentException(pagination + " does not support grouped QuerySpec");
        }
    }

    static List<String> readableProjections(QuerySpec spec, DynamicForm readableForm) {
        List<String> selected = projectionFields(spec, readableForm);
        return spec.projections().isEmpty() || spec.projections().equals(List.of("*")) ? selected
                : FormSqlRenderSupport.canonicalFieldNames(readableForm, selected);
    }

    /** 所有读取形状在已合并字段范围内展开通配符，计算列和关联路径留给各自的字段解析器。 */
    static List<String> projectionFields(QuerySpec spec, DynamicForm readableForm) {
        List<String> requested = spec.projections();
        if (!requested.isEmpty() && !requested.contains("*")) return requested;
        List<String> selected = new ArrayList<>(readableForm.fields().size() + requested.size());
        for (String name : requested.isEmpty() ? List.of("*") : requested) {
            if (name.equals("*")) {
                for (DynamicField field : readableForm.fields()) {
                    if (!spec.excludedFields().contains(field.identity().key())) selected.add(field.name());
                }
            } else selected.add(name);
        }
        if (selected.isEmpty()) {
            throw new IllegalArgumentException("projected query must select at least one field");
        }
        return List.copyOf(selected);
    }

    static List<String> readableGroups(QuerySpec spec, DynamicForm readableForm) {
        Objects.requireNonNull(readableForm, "readable form must not be null");
        if (spec.groups().isEmpty()) {
            return List.of();
        }
        return spec.groups().stream()
                   .map(readableForm::field)
                   .map(field -> {
                       requireUnencrypted(spec.form(), field.name(),
                                          "encrypted field must not be used for query grouping");
                       return field.name();
                   })
                   .toList();
    }

    static List<PageSort> readableSorts(DynamicForm form,
                                        DynamicForm readableForm,
                                        List<PageSort> sorts) {
        Objects.requireNonNull(sorts, "query sorts must not be null");
        if (sorts.isEmpty()) {
            return List.of();
        }
        return sorts.stream()
                      .map(sort -> {
                          var field = readableForm.field(sort.field());
                          requireUnencrypted(form, field.name(),
                                             "encrypted field must not be used for query ordering");
                          return field.name().equals(sort.field()) ? sort : new PageSort(field.name(), sort.direction());
                      })
                      .toList();
    }

    static void requireValidGrouping(List<String> projections,
                                     List<String> groups,
                                     List<PageSort> sorts) {
        if (groups.isEmpty()) {
            return;
        }
        Set<String> groupingFields = caseInsensitiveIndex(groups);
        for (String projection : projections) {
            if (!groupingFields.contains(projection)) {
                throw new IllegalArgumentException("grouped query projection must be a grouping field");
            }
        }
        for (PageSort sort : sorts) {
            if (!groupingFields.contains(sort.field())) {
                throw new IllegalArgumentException("grouped query sort must be a grouping field");
            }
        }
    }

    private static void requireUnencrypted(DynamicForm form, String field, String message) {
        if (form.protections().encrypted(field).isPresent()) {
            throw new IllegalArgumentException(message);
        }
    }

    private static Set<String> caseInsensitiveIndex(List<String> fields) {
        Set<String> index = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        index.addAll(fields);
        return index;
    }

    /** 计算别名使用结果元数据；原字段仍须通过可读范围和加密检查。 */
    static List<PageSort> readableExpressionSorts(QuerySpec spec, DynamicForm readableForm,
                                                  DynamicForm resultForm, List<PageSort> sorts) {
        return sorts.stream().map(sort -> {
            if (spec.expressions().containsKey(FieldIdentity.of(sort.field()).key())) {
                return new PageSort(resultForm.field(sort.field()).name(), sort.direction());
            }
            String field = readableForm.field(sort.field()).name();
            requireUnencrypted(spec.form(), field, "encrypted field must not be used for query ordering");
            return new PageSort(field, sort.direction());
        }).toList();
    }

    static void requireReadableUnprotectedKeysetSorts(
            DynamicForm form,
            DynamicForm readableForm,
            KeysetPageNormalizer.NormalizedKeysetPage page,
            SensitiveDisplayMode displayMode) {
        List<KeysetSort> sorts = page.sorts();
        for (KeysetSort sort : sorts) {
            requireKeysetField(form, readableForm, sort.field(), displayMode);
        }
    }

    static void requireKeysetField(DynamicForm form, DynamicForm readableForm,
                                    String field, SensitiveDisplayMode displayMode) {
        readableForm.field(field);
        requireUnencrypted(form, field, "encrypted field must not be used for keyset ordering");
        var masking = form.protections().masked(field).orElse(null);
        if (masking != null && (displayMode == SensitiveDisplayMode.MASKED
                || displayMode == SensitiveDisplayMode.DECLARED
                && masking.display() == SensitiveDisplayMode.MASKED)) {
            throw new IllegalArgumentException("masked field must not be used for keyset ordering");
        }
    }
}
