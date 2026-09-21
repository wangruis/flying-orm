package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorSort;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.rdb.form.spec.QuerySpec;

import java.util.List;
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
        return spec.projections().stream()
                   .map(readableForm::field)
                   .map(com.flying.orm.core.form.DynamicField::name)
                   .toList();
    }

    static List<String> readableGroups(QuerySpec spec, DynamicForm readableForm) {
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
        return Objects.requireNonNull(sorts, "query sorts must not be null").stream()
                      .map(sort -> {
                          var field = readableForm.field(sort.field());
                          requireUnencrypted(form, field.name(),
                                             "encrypted field must not be used for query ordering");
                          return new PageSort(field.name(), sort.direction());
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

    static void requireReadableUnencryptedCursorSorts(DynamicForm form,
                                                       DynamicForm readableForm,
                                                       List<CursorSort> sorts,
                                                       SensitiveDisplayMode displayMode) {
        for (var field : form.fields()) {
            if (field.primaryKey() && readableForm.findField(field.name()).isEmpty()) {
                throw new IllegalArgumentException(
                        "cursor pagination requires every primary-key field to be readable: " + field.name());
            }
        }
        for (CursorSort sort : sorts) {
            requireUnencrypted(form, sort.field(),
                               "encrypted field must not be used for cursor ordering");
            var masking = form.protections().masked(sort.field()).orElse(null);
            if (masking != null && (displayMode == SensitiveDisplayMode.MASKED
                    || displayMode == SensitiveDisplayMode.DECLARED
                    && masking.display() == SensitiveDisplayMode.MASKED)) {
                throw new IllegalArgumentException("masked field must not be used for cursor ordering");
            }
        }
    }

    static void requireCursorProjection(List<String> projections, List<CursorSort> sorts) {
        if (projections.isEmpty()) {
            return;
        }
        Set<String> projectedFields = caseInsensitiveIndex(projections);
        for (CursorSort sort : sorts) {
            if (!projectedFields.contains(sort.field())) {
                throw new IllegalArgumentException("cursor projection must include every cursor sort field");
            }
        }
    }

    static List<String> outputFields(List<String> projections, DynamicForm visibleForm) {
        if (projections.isEmpty()) {
            return visibleForm.fields().stream().map(com.flying.orm.core.form.DynamicField::name).toList();
        }
        return projections;
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

    static void requireReadableUnprotectedKeysetSorts(
            DynamicForm form,
            DynamicForm readableForm,
            KeysetPageNormalizer.NormalizedKeysetPage page,
            SensitiveDisplayMode displayMode) {
        List<KeysetSort> sorts = page.sorts();
        for (int index = 0; index < sorts.size(); index++) {
            KeysetSort sort = sorts.get(index);
            readableForm.field(sort.field());
            requireUnencrypted(form, sort.field(),
                               "encrypted field must not be used for keyset ordering");
            var masking = form.protections().masked(sort.field()).orElse(null);
            if (masking != null && (displayMode == SensitiveDisplayMode.MASKED
                    || displayMode == SensitiveDisplayMode.DECLARED
                    && masking.display() == SensitiveDisplayMode.MASKED)) {
                throw new IllegalArgumentException(
                        "masked field must not be used for keyset ordering");
            }
        }
    }
}
