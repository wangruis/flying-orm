package com.flying.orm.rdb.protection;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionNode;
import com.flying.orm.core.condition.LogicalOperator;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.scope.DataScope;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * 把显式保护搜索递归改写为隐藏盲索引条件。
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class ProtectedQueryRewriter {

    private final ProtectedSearchTokenService tokens;

    ProtectedQueryRewriter(ProtectedSearchTokenService tokens) {
        this.tokens = Objects.requireNonNull(tokens, "protected search token service must not be null");
    }

    /** 生成物理表单、已改写条件和仅业务可见的投影列表。 */
    ProtectedFieldRuntime.PreparedQuery prepare(DynamicForm form,
                                                 DynamicForm visibleForm,
                                                 ConditionGroup where,
                                                 DataScope scope,
                                                 ValueCodecRegistry codecs) {
        String tenant = ProtectedFieldValues.tenantIdentity(form, scope, codecs);
        return new ProtectedFieldRuntime.PreparedQuery(
                ProtectedFormLayout.physical(form),
                rewriteGroup(form, where, tenant, codecs),
                ProtectedFormLayout.visibleFieldNames(visibleForm));
    }

    /** 提取单个顶层 AND CONTAINS 条件，并继续改写其余保护条件。 */
    Optional<ProtectedFieldRuntime.PreparedContainsQuery> prepareContains(DynamicForm form,
                                                                          DynamicForm visibleForm,
                                                                          ConditionGroup where,
                                                                          DataScope scope,
                                                                          ValueCodecRegistry codecs) {
        ConditionGroup safeWhere = Objects.requireNonNull(where, "query where must not be null");
        if (!containsSearch(safeWhere)) {
            return Optional.empty();
        }
        if (safeWhere.operator() != LogicalOperator.AND) {
            throw new IllegalArgumentException("protected contains search requires a top-level AND condition");
        }
        TermCondition contains = null;
        ConditionGroup.Builder remaining = ConditionGroup.and();
        for (ConditionNode node : safeWhere.children()) {
            if (node instanceof TermCondition term && ProtectedConditions.CONTAINS.equals(term.operator())) {
                if (contains != null) {
                    throw new IllegalArgumentException("protected contains search supports one field per query");
                }
                contains = term;
            } else {
                if (node instanceof ConditionGroup nested && containsSearch(nested)) {
                    throw new IllegalArgumentException("protected contains search requires a top-level AND condition");
                }
                remaining.add(node);
            }
        }
        if (contains == null) {
            throw new IllegalArgumentException("protected contains search requires a top-level AND condition");
        }
        DynamicField field = form.findField(contains.field()).orElseThrow(
                () -> new IllegalArgumentException("protected search requires an encrypted field"));
        EncryptedFieldDefinition definition = form.protections().encrypted(field.name()).orElseThrow(
                () -> new IllegalArgumentException("protected search requires an encrypted field"));
        String tenant = ProtectedFieldValues.tenantIdentity(form, scope, codecs);
        String text = ProtectedFieldValues.encodedText(codecs, contains.value());
        ProtectedSearchTokenService.ContainsQuery query = tokens.containsQuery(
                text, definition, ProtectedFieldValues.context(form, field, tenant));
        ProtectedContainsLayout layout = ProtectedContainsLayout.resolve(form).orElseThrow();
        List<ProtectedFieldRuntime.ContainsTokenGroup> groups = new ArrayList<>(query.groups().size());
        query.groups().forEach(group -> groups.add(new ProtectedFieldRuntime.ContainsTokenGroup(
                group.keyVersion(), group.tokens())));
        return Optional.of(new ProtectedFieldRuntime.PreparedContainsQuery(
                ProtectedFormLayout.physical(form),
                rewriteGroup(form, remaining.build(), tenant, codecs),
                visibleFields(visibleForm, field.name()),
                form.protections().encryptedFields().keySet(),
                field.name(),
                ProtectedColumnNames.containsFieldTag(form.id(), field.name()),
                query.normalizedValue(),
                groups,
                query.distinctTokenCount(),
                form.fields().stream().filter(DynamicField::primaryKey).map(DynamicField::name).toList(),
                layout.table().table()));
    }

    boolean matchesContains(String value,
                            EncryptedFieldDefinition definition,
                            String normalizedQuery) {
        return tokens.matchesContains(value, definition, normalizedQuery);
    }

    private static List<String> visibleFields(DynamicForm visibleForm, String requiredField) {
        List<String> visible = new ArrayList<>(ProtectedFormLayout.visibleFieldNames(
                Objects.requireNonNull(visibleForm, "visible form must not be null")));
        if (!visible.contains(requiredField)) {
            visible.add(requiredField);
        }
        return List.copyOf(visible);
    }

    private static boolean containsSearch(ConditionNode node) {
        if (node instanceof TermCondition term) {
            return ProtectedConditions.CONTAINS.equals(term.operator());
        }
        ConditionGroup group = (ConditionGroup) node;
        return group.children().stream().anyMatch(ProtectedQueryRewriter::containsSearch);
    }

    private ConditionGroup rewriteGroup(DynamicForm form,
                                        ConditionGroup group,
                                        String tenant,
                                        ValueCodecRegistry codecs) {
        ConditionGroup safeGroup = Objects.requireNonNull(group, "query where must not be null");
        ConditionGroup.Builder builder = safeGroup.operator() == LogicalOperator.AND
                ? ConditionGroup.and() : ConditionGroup.or();
        for (ConditionNode node : safeGroup.children()) {
            builder.add(node instanceof ConditionGroup nested
                                ? rewriteGroup(form, nested, tenant, codecs)
                                : rewriteTerm(form, (TermCondition) node, tenant, codecs));
        }
        return builder.build();
    }

    private TermCondition rewriteTerm(DynamicForm form,
                                      TermCondition term,
                                      String tenant,
                                      ValueCodecRegistry codecs) {
        DynamicField field = form.findField(term.field()).orElse(null);
        EncryptedFieldDefinition definition = field == null
                ? null : form.protections().encrypted(field.name()).orElse(null);
        boolean protectedOperator = ProtectedConditions.EXACT.equals(term.operator())
                || ProtectedConditions.SUFFIX.equals(term.operator())
                || ProtectedConditions.CONTAINS.equals(term.operator());
        if (definition == null) {
            if (protectedOperator) {
                throw new IllegalArgumentException("protected search requires an encrypted field");
            }
            return term;
        }
        if ("is-null".equals(term.operator()) || "is-not-null".equals(term.operator())) {
            // 密文列与逻辑值共享 null 语义；无值判断不需要也不能生成搜索令牌。
            return term;
        }
        String text = ProtectedFieldValues.encodedText(codecs, term.value());
        ProtectedFieldContext context = ProtectedFieldValues.context(form, field, tenant);
        return switch (term.operator()) {
            case ProtectedConditions.EXACT -> TermCondition.of(
                    ProtectedFormLayout.exactColumn(form, field.name()), "in",
                    tokens.exactQueryTokens(text, definition, context, field.unique()));
            case ProtectedConditions.SUFFIX -> suffix(form, field, definition, text, context);
            case ProtectedConditions.CONTAINS -> throw new UnsupportedOperationException(
                    "protected contains search is not available in this execution path");
            default -> throw new IllegalArgumentException("encrypted field requires a protected search operator");
        };
    }

    private TermCondition suffix(DynamicForm form,
                                 DynamicField field,
                                 EncryptedFieldDefinition definition,
                                 String text,
                                 ProtectedFieldContext context) {
        ProtectedSearchTokenService.SuffixQuery suffix = tokens.suffixQuery(text, definition, context);
        return TermCondition.of(
                ProtectedFormLayout.suffixColumn(form, field.name(), suffix.length()), "in", suffix.tokens());
    }
}
