package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 把 CONTAINS 逻辑令牌计划渲染成按密钥版本隔离的候选查询。
 *
 * <p>每个版本单独执行 group/having，避免轮换期间把两个版本的局部令牌拼成一次错误命中。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class ProtectedContainsSqlPlanner {

    private final FormSqlRenderSupport support;
    ProtectedContainsSqlPlanner(FormSqlRenderSupport support) {
        this.support = Objects.requireNonNull(support, "form SQL render support must not be null");
    }

    SqlRequest rows(ProtectedFieldRuntime.PreparedContainsQuery query,
                    List<PageSort> sorts) {
        return rows(query, stableSorts(query, sorts), null, query.visibleFields());
    }

    SqlRequest rows(ProtectedFieldRuntime.PreparedContainsQuery query, List<PageSort> sorts,
                    FormQueryExpressions.Projection projection) {
        return rows(query, stableSorts(query, sorts, projection.resultForm()), null,
                query.visibleFields(), projection);
    }

    SqlRequest rows(ProtectedFieldRuntime.PreparedContainsQuery query,
                    KeysetPageNormalizer.NormalizedKeysetPage page, FormQueryExpressions.Projection projection) {
        for (com.flying.orm.core.page.KeysetSort sort : page.sorts()) {
            support.requireStableOffsetTimeOrdering(query.physicalForm().field(sort.field()));
        }
        return rows(query, List.of(), page, query.visibleFields(), projection);
    }

    String projectionField(String field) {
        return qualified("fop_business", field);
    }

    /** 计数只需核验搜索字段；不读取其他业务列，也不为候选结果排序。 */
    SqlRequest verificationRows(ProtectedFieldRuntime.PreparedContainsQuery query) {
        return rows(query, List.of(), null, List.of(query.fieldName()));
    }

    private SqlRequest rows(ProtectedFieldRuntime.PreparedContainsQuery query,
                            List<PageSort> sorts,
                            KeysetPageNormalizer.NormalizedKeysetPage cursor,
                            List<String> fields) {
        return rows(query, sorts, cursor, fields, null);
    }

    private SqlRequest rows(ProtectedFieldRuntime.PreparedContainsQuery query, List<PageSort> sorts,
                            KeysetPageNormalizer.NormalizedKeysetPage cursor, List<String> fields,
                            FormQueryExpressions.Projection projection) {
        SqlFragment selectedColumns = projection == null ? null : projection.columns();
        DynamicForm resultForm = projection == null ? query.physicalForm() : projection.resultForm();
        ProtectedFieldRuntime.PreparedContainsQuery safe = Objects.requireNonNull(
                query, "protected contains query must not be null");
        List<String> candidateColumns = candidateColumns(safe.primaryKeys().size());
        List<Object> parameters = new ArrayList<>();
        if (selectedColumns != null) {
            parameters.addAll(OwnedBindableValues.ownedValues(selectedColumns.parameters()));
        }
        List<String> candidates = new ArrayList<>(safe.tokenGroups().size());
        String fieldTag = ProtectedContainsLayout.fieldTagColumn(safe.primaryKeys());
        String tokenHash = ProtectedContainsLayout.tokenHashColumn(safe.primaryKeys());
        for (ProtectedFieldRuntime.ContainsTokenGroup group : safe.tokenGroups()) {
            candidates.add(candidateSubquery(safe, group, candidateColumns, fieldTag, tokenHash, parameters));
        }
        String businessAlias = "fop_business";
        SqlFragment where = businessCondition(safe, businessAlias);
        parameters.addAll(OwnedBindableValues.ownedValues(where.parameters()));
        String candidateAlias = "fop_candidate";
        String projections = selectedColumns != null ? selectedColumns.sql() : fields.stream()
                                 .map(field -> qualified(businessAlias, field) + " as " + support.identifier(field))
                                 .collect(Collectors.joining(", "));
        String joins = java.util.stream.IntStream.range(0, safe.primaryKeys().size())
                .mapToObj(index -> qualified(businessAlias, safe.primaryKeys().get(index)) + " = "
                        + qualified(candidateAlias, candidateColumns.get(index)))
                .collect(Collectors.joining(" and "));
        Function<String, String> sortField = field -> safe.physicalForm().findField(field).isPresent()
                ? qualified(businessAlias, safe.physicalForm().field(field).name())
                : support.identifier(resultForm.field(field).name());
        SqlFragment ordering = cursor != null
                ? new SqlFragment(FormKeysetSqlRenderer.orderBy(cursor, sortField), List.of())
                : projection != null ? projection.orderBy(sorts, sortField)
                : new SqlFragment(sorts.isEmpty() ? "" : sorts.stream()
                        .map(sort -> sortField.apply(sort.field()) + " " + sort.sqlKeyword())
                        .collect(Collectors.joining(", ", " order by ", "")), List.of());
        StringBuilder sql = new StringBuilder("select ").append(projections)
                .append(" from ").append(support.identifier(safe.physicalForm()))
                .append(' ').append(businessAlias)
                .append(" join (").append(String.join(" union ", candidates)).append(") ")
                .append(candidateAlias).append(" on ").append(joins);
        if (!where.sql().isBlank()) {
            sql.append(" where ").append(where.sql());
        }
        if (cursor != null && !cursor.firstPage()) {
            String seek = FormKeysetSqlRenderer.cursorPredicate(cursor,
                    field -> qualified(businessAlias, safe.physicalForm().field(field).name()),
                    (field, value) -> support.writeValue(safe.physicalForm().field(field), value), parameters);
            sql.append(where.sql().isBlank() ? " where " : " and ").append(seek);
        }
        if (!ordering.sql().isEmpty()) sql.append(' ').append(ordering.sql());
        parameters.addAll(ordering.parameters());
        // Execution options govern candidate rows and bytes; truncation here could hide matches or totals.
        return new SqlRequest(sql.toString(), parameters);
    }

    /**
     * 剩余业务条件必须限定到业务表别名。候选子查询使用内部列名承载主键，调用方合法业务字段可能与其同名；
     * 不限定来源会让四库都产生歧义列，甚至把条件错误地应用到候选关系。
     */
    private SqlFragment businessCondition(ProtectedFieldRuntime.PreparedContainsQuery query,
                                           String businessAlias) {
        ConditionGroup normalized = support.normalizeCondition(query.physicalForm(), query.remainingWhere());
        var renderer = support.normalizedConditionRenderer().withFieldIdentifierRenderer(name -> qualified(
                        businessAlias, query.physicalForm().field(name).name()), query.physicalForm()::field);
        return renderer.hasCorrelatedTerms()
                ? renderer.renderWhere(normalized,
                        name -> qualified(businessAlias, FormSqlRenderSupport.correlatedField(query.physicalForm(), name)),
                        name -> support.identifier(businessAlias))
                : renderer.renderWhere(normalized);
    }

    private String candidateSubquery(ProtectedFieldRuntime.PreparedContainsQuery query,
                                     ProtectedFieldRuntime.ContainsTokenGroup group,
                                     List<String> candidateColumns,
                                     String fieldTag,
                                     String tokenHash,
                                     List<Object> parameters) {
        String tokenAlias = "fop_token";
        List<byte[]> tokens = group.tokens();
        String markers = java.util.Collections.nCopies(tokens.size(), "?").stream()
                                      .collect(Collectors.joining(", "));
        String owners = java.util.stream.IntStream.range(0, query.primaryKeys().size())
                .mapToObj(index -> qualified(tokenAlias, query.primaryKeys().get(index)) + " as "
                        + support.identifier(candidateColumns.get(index)))
                .collect(Collectors.joining(", "));
        String groupedOwners = query.primaryKeys().stream()
                                    .map(field -> qualified(tokenAlias, field))
                                    .collect(Collectors.joining(", "));
        String tokenColumn = qualified(tokenAlias, tokenHash);
        parameters.add(query.fieldTag());
        parameters.addAll(tokens);
        parameters.add(query.distinctTokenCount());
        return "select " + owners + " from " + support.derivedRelationIdentifier(
                query.physicalForm(), query.tokenTable()) + " " + tokenAlias
                + " where " + qualified(tokenAlias, fieldTag) + " = ? and " + tokenColumn
                + " in (" + markers + ") group by " + groupedOwners
                + " having count(distinct " + tokenColumn + ") = ?";
    }

    private List<PageSort> stableSorts(ProtectedFieldRuntime.PreparedContainsQuery query,
                                       List<PageSort> sorts) {
        return stableSorts(query, sorts, query.physicalForm());
    }

    private List<PageSort> stableSorts(ProtectedFieldRuntime.PreparedContainsQuery query,
                                       List<PageSort> sorts, DynamicForm resultForm) {
        List<PageSort> result = new ArrayList<>(Objects.requireNonNull(sorts, "page sorts must not be null"));
        for (String primaryKey : query.primaryKeys()) {
            if (result.stream().noneMatch(sort -> resultForm.field(sort.field()).name()
                                                       .equals(primaryKey))) {
                result.add(PageSort.asc(primaryKey));
            }
        }
        for (PageSort sort : result) {
            DynamicField field = resultForm.field(sort.field());
            if (query.encryptedFields().stream().anyMatch(name -> name.equalsIgnoreCase(field.name()))) {
                throw new IllegalArgumentException("encrypted field cannot be used for protected contains ordering");
            }
            support.requireStableOffsetTimeOrdering(field);
        }
        return List.copyOf(result);
    }

    private String qualified(String alias, String field) {
        return alias + "." + support.identifier(field);
    }

    private static List<String> candidateColumns(int count) {
        return java.util.stream.IntStream.range(0, count)
                .mapToObj(index -> "__fop_c" + index)
                .toList();
    }

}
