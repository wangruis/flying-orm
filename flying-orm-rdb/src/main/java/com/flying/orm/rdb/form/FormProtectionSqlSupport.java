package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.PaginationDialect;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.result.DynamicRow;

import java.util.LinkedHashMap;
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

    FormPreparedWrite prepareWrite(DynamicForm form,
                                   Map<String, Object> values,
                                   DataScope scope) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        return prepareWrite(safeForm, protectedFields.physicalForm(safeForm), values, scope);
    }

    FormPreparedWrite prepareWrite(DynamicForm form,
                                   DynamicForm physicalForm,
                                   Map<String, Object> values,
                                   DataScope scope) {
        return writeOperation(form, physicalForm, scope, null).prepare(values);
    }

    /** 为批量回执生成受保护字段的稳定、非明文身份。 */
    Map<String, byte[]> prepareReceiptIdentities(DynamicForm form,
                                                 Map<String, Object> values,
                                                 DataScope scope) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        return writeOperation(
                safeForm, protectedFields.physicalForm(safeForm), scope, null).receiptIdentities(values);
    }

    Optional<ProtectedWriteWork> protectedWrite(DynamicForm form,
                                                Map<String, Object> logicalValues,
                                                DataScope scope,
                                                SqlRequest writeRequest,
                                                ProtectedFieldRuntime.PreparedQuery ownerQuery,
                                                ProtectedWriteWork.Kind kind) {
        return protectedWrite(form, logicalValues, scope, writeRequest, ownerQuery, kind,
                              ProtectedContainsLayout.resolve(form).orElse(null));
    }

    Optional<ProtectedWriteWork> protectedWrite(DynamicForm form,
                                                Map<String, Object> logicalValues,
                                                DataScope scope,
                                                SqlRequest writeRequest,
                                                ProtectedFieldRuntime.PreparedQuery ownerQuery,
                                                ProtectedWriteWork.Kind kind,
                                                ProtectedContainsLayout layout) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        return writeOperation(
                safeForm, protectedFields.physicalForm(safeForm), scope, layout).protectedWrite(
                        logicalValues, writeRequest, ownerQuery, kind);
    }

    WriteOperation writeOperation(DynamicForm form,
                                  DynamicForm physicalForm,
                                  DataScope scope,
                                  ProtectedContainsLayout layout) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        DynamicForm safePhysicalForm = Objects.requireNonNull(
                physicalForm, "physical form must not be null");
        return new WriteOperation(
                safeForm,
                safePhysicalForm,
                protectedFields.writeOperation(safeForm, safePhysicalForm, scope, support.valueCodecs),
                layout == null ? null : new SideIndexPlan(safeForm, layout));
    }

    private Map<String, Object> canonicalValues(DynamicForm form, Map<String, Object> values) {
        Map<String, Object> safeValues = Objects.requireNonNull(values, "protected write values must not be null");
        Map<String, Object> canonical = new LinkedHashMap<>(Math.max(16, safeValues.size() * 2));
        for (Map.Entry<String, Object> entry : safeValues.entrySet()) {
            String field = support.field(form, entry.getKey()).name();
            if (canonical.containsKey(field)) {
                throw new IllegalArgumentException("duplicate normalized protected write field");
            }
            canonical.put(field, entry.getValue());
        }
        return canonical;
    }

    final class WriteOperation {

        private final DynamicForm form;
        private final DynamicForm physicalForm;
        private final boolean protectsValues;
        private final ProtectedFieldRuntime.WriteOperation values;
        private final SideIndexPlan sideIndex;

        private WriteOperation(DynamicForm form,
                               DynamicForm physicalForm,
                               ProtectedFieldRuntime.WriteOperation values,
                               SideIndexPlan sideIndex) {
            this.form = form;
            this.physicalForm = physicalForm;
            this.protectsValues = !form.protections().encryptedFields().isEmpty();
            this.values = values;
            this.sideIndex = sideIndex;
        }

        FormPreparedWrite prepare(Map<String, Object> logicalValues) {
            Map<String, Object> safeValues = Objects.requireNonNull(
                    logicalValues, "dynamic form values must not be null");
            if (!protectsValues) {
                return new FormPreparedWrite(physicalForm, safeValues);
            }
            ProtectedFieldRuntime.PreparedWrite prepared = values.prepare(safeValues);
            return new FormPreparedWrite(prepared.physicalForm(), prepared.ownedValues());
        }

        Map<String, byte[]> receiptIdentities(Map<String, Object> logicalValues) {
            return values.receiptIdentities(logicalValues);
        }

        SqlRequest update(FormPreparedWrite write,
                          ConditionGroup where,
                          OptimisticLockOptions lock) {
            return FormProtectionSqlSupport.this.update(write, where, lock);
        }

        Optional<ProtectedWriteWork> protectedWrite(Map<String, Object> logicalValues,
                                                    SqlRequest writeRequest,
                                                    ProtectedFieldRuntime.PreparedQuery ownerQuery,
                                                    ProtectedWriteWork.Kind kind) {
            if (sideIndex == null) {
                return Optional.empty();
            }
            Map<String, Object> canonical = canonicalValues(form, logicalValues);
            if (kind == ProtectedWriteWork.Kind.UPDATE
                    && sideIndex.owners.stream().anyMatch(canonical::containsKey)) {
                throw new IllegalArgumentException(
                        "protected contains update must not change primary key");
            }
            List<ProtectedFieldRuntime.ContainsFieldTokens> tokens = values.containsTokens(canonical);
            if (tokens.isEmpty()) {
                return Optional.empty();
            }
            Map<String, Object> knownOwner = new LinkedHashMap<>();
            if (kind != ProtectedWriteWork.Kind.UPDATE) {
                sideIndex.owners.forEach(field -> {
                    if (canonical.containsKey(field)) {
                        knownOwner.put(field, canonical.get(field));
                    }
                });
            }
            SqlRequest ownerRequest = ownerQuery == null ? null : queries.selectProjected(
                    ownerQuery.physicalForm(), ownerQuery.where(), sideIndex.owners, List.of(), List.of());
            List<ProtectedWriteWork.FieldTokens> fields = tokens.stream()
                    .map(field -> ProtectedWriteWork.FieldTokens.owned(
                            field.fieldTag(), field.tokenCount(), field::ownedToken))
                    .toList();
            return Optional.of(new ProtectedWriteWork(
                    kind,
                    writeRequest,
                    ownerRequest,
                    sideIndex.owners,
                    knownOwner,
                    sideIndex.ownerPredicate,
                    sideIndex.deleteSql,
                    sideIndex.insertSql,
                    fields));
        }
    }

    private final class SideIndexPlan {

        private final List<String> owners;
        private final String ownerPredicate;
        private final String deleteSql;
        private final String insertSql;

        private SideIndexPlan(DynamicForm form, ProtectedContainsLayout layout) {
            this.owners = form.fields().stream()
                              .filter(com.flying.orm.core.form.DynamicField::primaryKey)
                              .map(com.flying.orm.core.form.DynamicField::name)
                              .toList();
            this.ownerPredicate = owners.stream()
                                        .map(field -> support.identifier(field) + " = ?")
                                        .collect(java.util.stream.Collectors.joining(" and "));
            String columns = owners.stream().map(support::identifier)
                                   .collect(java.util.stream.Collectors.joining(", "));
            String markers = java.util.Collections.nCopies(owners.size() + 2, "?").stream()
                                           .collect(java.util.stream.Collectors.joining(", "));
            String tokenTable = support.identifier(layout.table().table());
            this.deleteSql = "delete from " + tokenTable + " where " + ownerPredicate
                    + " and " + support.identifier("field_tag") + " = ?";
            this.insertSql = "insert into " + tokenTable + " (" + columns + ", "
                    + support.identifier("field_tag") + ", " + support.identifier("token_hash")
                    + ") values (" + markers + ")";
        }
    }

    ProtectedFieldRuntime.PreparedQuery prepareQuery(DynamicForm form,
                                                      DynamicForm visibleForm,
                                                      ConditionGroup where,
                                                      DataScope scope) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        return prepareQuery(safeForm, protectedFields.physicalForm(safeForm), visibleForm, where, scope);
    }

    ProtectedFieldRuntime.PreparedQuery prepareQuery(DynamicForm form,
                                                      DynamicForm physicalForm,
                                                      DynamicForm visibleForm,
                                                      ConditionGroup where,
                                                      DataScope scope) {
        return protectedFields.prepareQuery(
                form, physicalForm, visibleForm, FormDataScopes.unwrapTrustedValues(form, where, scope),
                scope, support.valueCodecs);
    }

    Optional<ProtectedFieldRuntime.PreparedContainsQuery> prepareContainsQuery(DynamicForm form,
                                                                                DynamicForm visibleForm,
                                                                                ConditionGroup where,
                                                                                DataScope scope) {
        return protectedFields.prepareContainsQuery(
                form, visibleForm, FormDataScopes.unwrapTrustedValues(form, where, scope), scope, support.valueCodecs);
    }

    /** 字段出现即需要维护侧索引；null 也必须捕获 owner 以删除旧 token。 */
    boolean requiresOwnerQuery(DynamicForm form, Map<String, Object> logicalValues) {
        if (form.protections().encryptedFields().isEmpty()) {
            return false;
        }
        for (String name : logicalValues.keySet()) {
            var definition = form.protections().encrypted(support.field(form, name).name()).orElse(null);
            if (definition != null && definition.searchModes().contains(EncryptedSearchMode.CONTAINS)) {
                return true;
            }
        }
        return false;
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

    SqlRequest containsRows(ProtectedFieldRuntime.PreparedContainsQuery query,
                            CursorPageNormalizer.NormalizedCursorPage page,
                            int candidateLimit) {
        return contains.rows(query, page, candidateLimit);
    }

    SqlRequest insert(FormPreparedWrite write) {
        return writes.insert(write.physicalForm(), write.values());
    }

    SqlRequest update(FormPreparedWrite write, ConditionGroup where) {
        return writes.update(write.physicalForm(), write.values(), where);
    }

    SqlRequest update(FormPreparedWrite write,
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

    SqlRequest select(ProtectedFieldRuntime.PreparedQuery query,
                      CursorPageNormalizer.NormalizedCursorPage page) {
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

    ProtectedFieldRuntime.ResultOperation resultOperation(DynamicForm form,
                                                          DataScope scope,
                                                          SensitiveDisplayMode displayMode) {
        return protectedFields.resultOperation(form, scope, displayMode, support.valueCodecs);
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

}
