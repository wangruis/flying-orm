package com.flying.orm.rdb.form;

import com.flying.orm.core.page.CursorDirection;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.PaginationDialect;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 把 CONTAINS 逻辑令牌计划渲染成按密钥版本隔离的有界候选查询。
 *
 * <p>每个版本单独执行 group/having，避免轮换期间把两个版本的局部令牌拼成一次错误命中。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class ProtectedContainsSqlPlanner {

    private static final int MAX_IN_LIST_VALUES = 1000;
    private static final int MAX_SQL_PARAMETERS = 2100;
    private static final String PARAMETER_LIMIT_MESSAGE =
            "protected contains query exceeds the portable SQL parameter limit";

    private final FormSqlRenderSupport support;
    private final PaginationDialect pagination;

    ProtectedContainsSqlPlanner(FormSqlRenderSupport support, PaginationDialect pagination) {
        this.support = Objects.requireNonNull(support, "form SQL render support must not be null");
        this.pagination = Objects.requireNonNull(pagination, "pagination dialect must not be null");
    }

    List<SqlRequest> candidates(ProtectedFieldRuntime.PreparedContainsQuery query, int candidateLimit) {
        ProtectedFieldRuntime.PreparedContainsQuery safe = Objects.requireNonNull(
                query, "protected contains query must not be null");
        if (candidateLimit < 1 || candidateLimit >= Integer.MAX_VALUE) {
            throw new IllegalArgumentException("protected contains candidate limit must be positive and bounded");
        }
        String owners = safe.primaryKeys().stream().map(support::identifier).collect(Collectors.joining(", "));
        String tokenColumn = support.identifier("token_hash");
        String base = "select " + owners + " from " + support.identifier(safe.tokenTable())
                + " where " + support.identifier("field_tag") + " = ? and " + tokenColumn + " in (%s)"
                + " group by " + owners
                + " having count(distinct " + tokenColumn + ") = ?"
                + " order by " + owners;
        List<SqlRequest> requests = new ArrayList<>(safe.tokenGroups().size());
        for (ProtectedFieldRuntime.ContainsTokenGroup group : safe.tokenGroups()) {
            List<byte[]> tokens = group.tokens();
            requireTokenCount(tokens.size());
            String markers = java.util.Collections.nCopies(tokens.size(), "?").stream()
                                          .collect(Collectors.joining(", "));
            List<Object> parameters = new ArrayList<>(tokens.size() + 2);
            parameters.add(safe.fieldTag());
            parameters.addAll(tokens);
            parameters.add(safe.distinctTokenCount());
            requests.add(requireParameterCount(
                    pagination.limit(base.formatted(markers), parameters, candidateLimit + 1)));
        }
        return List.copyOf(requests);
    }

    SqlRequest rows(ProtectedFieldRuntime.PreparedContainsQuery query,
                    List<PageSort> sorts,
                    int candidateLimit) {
        return rows(query, sorts, null, candidateLimit);
    }

    SqlRequest rows(ProtectedFieldRuntime.PreparedContainsQuery query,
                    CursorPageQuery page,
                    int candidateLimit) {
        CursorPageQuery safePage = Objects.requireNonNull(page, "cursor page query must not be null");
        List<PageSort> sorts = safePage.sorts().stream()
                .map(sort -> sort.direction() == CursorDirection.ASC
                        ? PageSort.asc(sort.field()) : PageSort.desc(sort.field()))
                .toList();
        return rows(query, sorts, safePage, candidateLimit);
    }

    private SqlRequest rows(ProtectedFieldRuntime.PreparedContainsQuery query,
                            List<PageSort> sorts,
                            CursorPageQuery cursor,
                            int candidateLimit) {
        ProtectedFieldRuntime.PreparedContainsQuery safe = Objects.requireNonNull(
                query, "protected contains query must not be null");
        requireCandidateLimit(candidateLimit);
        List<String> candidateColumns = candidateColumns(safe.primaryKeys().size());
        List<Object> parameters = new ArrayList<>();
        List<String> candidates = new ArrayList<>(safe.tokenGroups().size());
        for (ProtectedFieldRuntime.ContainsTokenGroup group : safe.tokenGroups()) {
            candidates.add(candidateSubquery(safe, group, candidateColumns, parameters));
        }
        String businessAlias = "fop_business";
        SqlFragment where = businessCondition(safe, businessAlias);
        parameters.addAll(where.parameters());
        String candidateAlias = "fop_candidate";
        String projections = safe.visibleFields().stream()
                                 .map(field -> qualified(businessAlias, field) + " as " + support.identifier(field))
                                 .collect(Collectors.joining(", "));
        String joins = java.util.stream.IntStream.range(0, safe.primaryKeys().size())
                .mapToObj(index -> qualified(businessAlias, safe.primaryKeys().get(index)) + " = "
                        + qualified(candidateAlias, candidateColumns.get(index)))
                .collect(Collectors.joining(" and "));
        List<PageSort> stableSorts = stableSorts(safe, sorts);
        String orderBy = stableSorts.stream()
                                    .map(sort -> qualified(businessAlias,
                                                           safe.physicalForm().field(sort.field()).name())
                                            + " " + sort.sqlKeyword())
                                    .collect(Collectors.joining(", ", " order by ", ""));
        StringBuilder sql = new StringBuilder("select ").append(projections)
                .append(" from ").append(support.identifier(safe.physicalForm().table()))
                .append(' ').append(businessAlias)
                .append(" join (").append(String.join(" union ", candidates)).append(") ")
                .append(candidateAlias).append(" on ").append(joins);
        if (!where.sql().isBlank()) {
            sql.append(" where ").append(where.sql());
        }
        if (cursor != null && !cursor.firstPage()) {
            sql.append(where.sql().isBlank() ? " where (" : " and (")
               .append(cursorWhere(safe, businessAlias, cursor, parameters))
               .append(')');
        }
        sql.append(orderBy);
        return requireParameterCount(pagination.limit(sql.toString(), parameters, candidateLimit + 1));
    }

    /**
     * 剩余业务条件必须限定到业务表别名。候选子查询使用内部列名承载主键，调用方合法业务字段可能与其同名；
     * 不限定来源会让四库都产生歧义列，甚至把条件错误地应用到候选关系。
     */
    private SqlFragment businessCondition(ProtectedFieldRuntime.PreparedContainsQuery query,
                                           String businessAlias) {
        return support.conditionRenderer.withFieldIdentifierRenderer(name -> qualified(
                        businessAlias, query.physicalForm().field(name).name()))
                .renderWhere(query.remainingWhere());
    }

    private String cursorWhere(ProtectedFieldRuntime.PreparedContainsQuery query,
                               String alias,
                               CursorPageQuery page,
                               List<Object> parameters) {
        java.util.StringJoiner alternatives = new java.util.StringJoiner(" or ");
        List<Object> cursor = page.cursor();
        for (int pivot = 0; pivot < page.sorts().size(); pivot++) {
            java.util.StringJoiner terms = new java.util.StringJoiner(" and ");
            for (int prefix = 0; prefix < pivot; prefix++) {
                String field = query.physicalForm().field(page.sorts().get(prefix).field()).name();
                terms.add(qualified(alias, field) + " = ?");
                parameters.add(support.valueCodecs.write(cursor.get(prefix)));
            }
            com.flying.orm.core.page.CursorSort sort = page.sorts().get(pivot);
            String field = query.physicalForm().field(sort.field()).name();
            terms.add(qualified(alias, field) + (sort.direction() == CursorDirection.ASC ? " > ?" : " < ?"));
            parameters.add(support.valueCodecs.write(cursor.get(pivot)));
            alternatives.add(pivot == 0 ? terms.toString() : "(" + terms + ")");
        }
        return alternatives.toString();
    }

    private String candidateSubquery(ProtectedFieldRuntime.PreparedContainsQuery query,
                                     ProtectedFieldRuntime.ContainsTokenGroup group,
                                     List<String> candidateColumns,
                                     List<Object> parameters) {
        String tokenAlias = "fop_token";
        List<byte[]> tokens = group.tokens();
        requireTokenCount(tokens.size());
        String markers = java.util.Collections.nCopies(tokens.size(), "?").stream()
                                      .collect(Collectors.joining(", "));
        String owners = java.util.stream.IntStream.range(0, query.primaryKeys().size())
                .mapToObj(index -> qualified(tokenAlias, query.primaryKeys().get(index)) + " as "
                        + support.identifier(candidateColumns.get(index)))
                .collect(Collectors.joining(", "));
        String groupedOwners = query.primaryKeys().stream()
                                    .map(field -> qualified(tokenAlias, field))
                                    .collect(Collectors.joining(", "));
        String tokenColumn = qualified(tokenAlias, "token_hash");
        parameters.add(query.fieldTag());
        parameters.addAll(tokens);
        parameters.add(query.distinctTokenCount());
        return "select " + owners + " from " + support.identifier(query.tokenTable()) + " " + tokenAlias
                + " where " + qualified(tokenAlias, "field_tag") + " = ? and " + tokenColumn
                + " in (" + markers + ") group by " + groupedOwners
                + " having count(distinct " + tokenColumn + ") = ?";
    }

    private List<PageSort> stableSorts(ProtectedFieldRuntime.PreparedContainsQuery query,
                                       List<PageSort> sorts) {
        List<PageSort> result = new ArrayList<>(Objects.requireNonNull(sorts, "page sorts must not be null"));
        for (PageSort sort : result) {
            String field = query.physicalForm().field(sort.field()).name();
            if (query.encryptedFields().stream().anyMatch(name -> name.equalsIgnoreCase(field))) {
                throw new IllegalArgumentException("encrypted field cannot be used for protected contains ordering");
            }
        }
        for (String primaryKey : query.primaryKeys()) {
            if (result.stream().noneMatch(sort -> query.physicalForm().field(sort.field()).name()
                                                       .equals(primaryKey))) {
                result.add(PageSort.asc(primaryKey));
            }
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

    private static void requireCandidateLimit(int candidateLimit) {
        if (candidateLimit < 1 || candidateLimit >= Integer.MAX_VALUE) {
            throw new IllegalArgumentException("protected contains candidate limit must be positive and bounded");
        }
    }

    private static void requireTokenCount(int tokenCount) {
        if (tokenCount > MAX_IN_LIST_VALUES) {
            throw new IllegalArgumentException(PARAMETER_LIMIT_MESSAGE);
        }
    }

    private static SqlRequest requireParameterCount(SqlRequest request) {
        if (request.parameters().size() > MAX_SQL_PARAMETERS) {
            throw new IllegalArgumentException(PARAMETER_LIMIT_MESSAGE);
        }
        return request;
    }
}
