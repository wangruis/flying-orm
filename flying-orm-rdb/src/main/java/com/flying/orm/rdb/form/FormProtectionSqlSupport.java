package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.dialect.PaginationDialect;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.result.DynamicRow;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 集中维护受保护字段与普通表单 SQL 渲染器之间的包内协作。
 *
 * <p>该类型只做纯值转换和 SQL 请求装配，不获取连接、不控制事务。把这部分职责从公共渲染门面拆出，
 * 可以让单表、JOIN、批量和后续侧索引工作单元复用同一套物理表单与结果转换语义。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class FormProtectionSqlSupport {

    private final FormSqlRenderSupport support;
    private final FormQuerySqlRenderer queries;
    private final FormWriteSqlRenderer writes;
    private final ProtectedFieldRuntime protectedFields;
    private final ProtectedContainsSqlPlanner contains;

    FormProtectionSqlSupport(FormSqlRenderSupport support,
                             FormQuerySqlRenderer queries,
                             FormWriteSqlRenderer writes,
                             ProtectedFieldRuntime protectedFields,
                             PaginationDialect pagination) {
        this.support = Objects.requireNonNull(support, "form SQL render support must not be null");
        this.queries = Objects.requireNonNull(queries, "form query SQL renderer must not be null");
        this.writes = Objects.requireNonNull(writes, "form write SQL renderer must not be null");
        this.protectedFields = Objects.requireNonNull(
                protectedFields, "protected field runtime must not be null");
        this.contains = new ProtectedContainsSqlPlanner(this.support, pagination);
    }

    ProtectedFieldRuntime.PreparedWrite prepareWrite(DynamicForm form,
                                                      Map<String, Object> values,
                                                      DataScope scope) {
        return protectedFields.prepareWrite(form, values, scope, support.valueCodecs);
    }

    /** 为批量回执生成受保护字段的稳定、非明文身份。 */
    Map<String, byte[]> prepareReceiptIdentities(DynamicForm form,
                                                 Map<String, Object> values,
                                                 DataScope scope) {
        return protectedFields.prepareReceiptIdentities(form, values, scope, support.valueCodecs);
    }

    Optional<ProtectedWriteWork> protectedWrite(DynamicForm form,
                                                Map<String, Object> logicalValues,
                                                DataScope scope,
                                                SqlRequest writeRequest,
                                                ProtectedFieldRuntime.PreparedQuery ownerQuery,
                                                ProtectedWriteWork.Kind kind) {
        if (kind == ProtectedWriteWork.Kind.UPDATE
                && hasContainsIndex(form)
                && form.fields().stream().anyMatch(field -> field.primaryKey()
                        && logicalValues.containsKey(field.name()))) {
            throw new IllegalArgumentException(
                    "protected contains update must not change primary key");
        }
        List<ProtectedFieldRuntime.ContainsFieldTokens> tokens = protectedFields.prepareContainsTokens(
                form, logicalValues, scope, support.valueCodecs);
        if (tokens.isEmpty()) {
            return Optional.empty();
        }
        ProtectedContainsLayout layout = ProtectedContainsLayout.resolve(form).orElseThrow();
        List<String> owners = form.fields().stream()
                                  .filter(com.flying.orm.core.form.DynamicField::primaryKey)
                                  .map(com.flying.orm.core.form.DynamicField::name)
                                  .toList();
        Map<String, Object> knownOwner = new java.util.LinkedHashMap<>();
        if (kind != ProtectedWriteWork.Kind.UPDATE) {
            owners.forEach(field -> {
                if (logicalValues.containsKey(field)) {
                    knownOwner.put(field, logicalValues.get(field));
                }
            });
        }
        String ownerPredicate = owners.stream()
                                      .map(field -> support.identifier(field) + " = ?")
                                      .collect(java.util.stream.Collectors.joining(" and "));
        String columns = owners.stream().map(support::identifier)
                               .collect(java.util.stream.Collectors.joining(", "));
        String markers = java.util.Collections.nCopies(owners.size() + 2, "?").stream()
                                       .collect(java.util.stream.Collectors.joining(", "));
        String tokenTable = support.identifier(layout.table().table());
        SqlRequest ownerRequest = ownerQuery == null ? null : queries.selectProjected(
                ownerQuery.physicalForm(), ownerQuery.where(), owners, List.of(), List.of());
        List<ProtectedWriteWork.FieldTokens> fields = tokens.stream()
                .map(field -> new ProtectedWriteWork.FieldTokens(field.fieldTag(), field.tokens()))
                .toList();
        return Optional.of(new ProtectedWriteWork(
                kind,
                writeRequest,
                ownerRequest,
                owners,
                knownOwner,
                ownerPredicate,
                "delete from " + tokenTable + " where " + ownerPredicate
                        + " and " + support.identifier("field_tag") + " = ?",
                "insert into " + tokenTable + " (" + columns + ", "
                        + support.identifier("field_tag") + ", " + support.identifier("token_hash")
                        + ") values (" + markers + ")",
                fields));
    }

    ProtectedFieldRuntime.PreparedQuery prepareQuery(DynamicForm form,
                                                      DynamicForm visibleForm,
                                                      ConditionGroup where,
                                                      DataScope scope) {
        return protectedFields.prepareQuery(form, visibleForm, where, scope, support.valueCodecs);
    }

    Optional<ProtectedFieldRuntime.PreparedContainsQuery> prepareContainsQuery(DynamicForm form,
                                                                                DynamicForm visibleForm,
                                                                                ConditionGroup where,
                                                                                DataScope scope) {
        return protectedFields.prepareContainsQuery(form, visibleForm, where, scope, support.valueCodecs);
    }

    Optional<ProtectedFieldRuntime.PreparedContainsQuery> prepareContainsQuery(DynamicForm form,
                                                                                ConditionGroup where,
                                                                                DataScope scope) {
        return prepareContainsQuery(form, form, where, scope);
    }

    List<SqlRequest> containsCandidates(ProtectedFieldRuntime.PreparedContainsQuery query, int candidateLimit) {
        return contains.candidates(query, candidateLimit);
    }

    SqlRequest containsRows(ProtectedFieldRuntime.PreparedContainsQuery query,
                            List<PageSort> sorts,
                            int candidateLimit) {
        return contains.rows(query, sorts, candidateLimit);
    }

    SqlRequest containsRows(ProtectedFieldRuntime.PreparedContainsQuery query,
                            CursorPageQuery page,
                            int candidateLimit) {
        return contains.rows(query, page, candidateLimit);
    }

    SqlRequest insert(ProtectedFieldRuntime.PreparedWrite write) {
        return writes.insert(write.physicalForm(), write.values());
    }

    SqlRequest update(ProtectedFieldRuntime.PreparedWrite write, ConditionGroup where) {
        return writes.update(write.physicalForm(), write.values(), where);
    }

    SqlRequest update(ProtectedFieldRuntime.PreparedWrite write,
                      ConditionGroup where,
                      OptimisticLockOptions lock) {
        return writes.update(write.physicalForm(), write.values(), where, lock);
    }

    SqlRequest delete(ProtectedFieldRuntime.PreparedQuery query, OptimisticLockOptions lock) {
        return lock == null
                ? writes.delete(query.physicalForm(), query.where())
                : writes.delete(query.physicalForm(), query.where(), lock);
    }

    SqlRequest select(ProtectedFieldRuntime.PreparedQuery query,
                      List<String> groups,
                      List<PageSort> sorts) {
        return queries.selectProjected(query.physicalForm(), query.where(), query.visibleFields(), groups, sorts);
    }

    SqlRequest select(ProtectedFieldRuntime.PreparedQuery query, PageQuery page) {
        return queries.selectPhysical(query.physicalForm(), query.visibleFields(), query.where(), page);
    }

    SqlRequest select(ProtectedFieldRuntime.PreparedQuery query, CursorPageQuery page) {
        return queries.selectPhysical(query.physicalForm(), query.visibleFields(), query.where(), page);
    }

    SqlRequest count(ProtectedFieldRuntime.PreparedQuery query) {
        return queries.count(query.physicalForm(), query.where());
    }

    DynamicRow transform(DynamicForm form,
                         DynamicRow row,
                         DataScope scope,
                         SensitiveDisplayMode displayMode) {
        return protectedFields.transformResult(form, row, scope, displayMode, support.valueCodecs);
    }

    boolean matchesContains(DynamicForm form,
                            ProtectedFieldRuntime.PreparedContainsQuery query,
                            DynamicRow decryptedRow) {
        return protectedFields.matchesContains(form, query, decryptedRow);
    }

    DynamicRow mask(DynamicForm form,
                    DynamicRow decryptedRow,
                    SensitiveDisplayMode displayMode) {
        return protectedFields.maskResult(form, decryptedRow, displayMode);
    }

    DynamicForm physicalForm(DynamicForm form) {
        return protectedFields.physicalForm(form);
    }

    boolean hasContainsIndex(DynamicForm form) {
        return Objects.requireNonNull(form, "dynamic form must not be null")
                      .protections().encryptedFields().values().stream()
                      .anyMatch(definition -> definition.searchModes().contains(
                              com.flying.orm.core.protection.EncryptedSearchMode.CONTAINS));
    }
}
