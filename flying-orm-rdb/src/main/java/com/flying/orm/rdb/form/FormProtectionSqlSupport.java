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
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.rdb.dialect.PaginationDialect;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.internal.binding.SqlNullParameter;
import com.flying.orm.rdb.lock.LockingReadDialect;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.lock.ReadLock;
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
    final ProtectedContainsSqlPlanner contains;

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

    WriteOperation writeOperation(DynamicForm form,
                                  DynamicForm physicalForm,
                                  DataScope scope) {
        return writeOperation(form, physicalForm, scope,
                              ProtectedContainsLayout.resolve(form).orElse(null));
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

        SqlRequest update(FormPreparedWrite write,
                          ConditionGroup where,
                          OptimisticLockOptions lock) {
            return FormProtectionSqlSupport.this.update(write, where, lock);
        }

        Optional<ProtectedWriteWork> protectedWrite(Map<String, Object> logicalValues,
                                                    SqlRequest writeRequest,
                                                    ProtectedFieldRuntime.PreparedQuery ownerQuery,
                                                    ProtectedWriteWork.Kind kind,
                                                    Map<String, Object> knownOwner) {
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

        /** 单条 INSERT 的绑定顺序与已准备字段顺序一致；只取最终参数，不再执行 codec。 */
        Map<String, Object> insertOwner(FormPreparedWrite write, SqlRequest request) {
            if (sideIndex == null) {
                return Map.of();
            }
            Map<String, Object> owner = new LinkedHashMap<>();
            List<Object> parameters = OwnedBindableValues.ownedValues(request.parameters());
            int index = 0;
            for (String column : write.values().keySet()) {
                var field = write.physicalForm().field(column);
                if (field.primaryKey()) {
                    Object value = parameters.get(index);
                    owner.put(field.name(), value instanceof SqlNullParameter ? null : value);
                }
                index++;
            }
            return owner;
        }

        /** 批量复用已有字段索引，仅定位 owner 分量，后续行不重复扫描整行字段。 */
        Map<String, Object> insertOwner(BatchInsertPlan plan, Object[] parameters) {
            Map<String, Object> owner = new LinkedHashMap<>();
            for (String name : sideIndex.owners) {
                String normalized = physicalForm.field(name).normalizedName();
                Integer index = plan.columnLayout().indexesByNormalizedName().get(normalized);
                if (index != null) {
                    int parameterIndex = index < plan.scopeParameterIndex()
                            ? index : index + plan.scopeParameters().size();
                    Object value = parameters[parameterIndex];
                    owner.put(name, value instanceof SqlNullParameter ? null : value);
                }
            }
            return owner;
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
            String tokenTable = support.identifier(layout.table());
            String fieldTag = support.identifier(ProtectedContainsLayout.fieldTagColumn(owners));
            String tokenHash = support.identifier(ProtectedContainsLayout.tokenHashColumn(owners));
            this.deleteSql = "delete from " + tokenTable + " where " + ownerPredicate
                    + " and " + fieldTag + " = ?";
            this.insertSql = "insert into " + tokenTable + " (" + columns + ", "
                    + fieldTag + ", " + tokenHash
                    + ") values (" + markers + ")";
        }
    }

    /** 最终投影由查询规划直接传入；纯条件路径传空列表，不遍历无用业务字段。 */
    ProtectedFieldRuntime.PreparedQuery prepareQuery(DynamicForm form,
                                                      ConditionGroup where,
                                                      DataScope scope,
                                                      List<String> visibleFields) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        return prepareQuery(
                safeForm, protectedFields.physicalForm(safeForm), where, scope, visibleFields);
    }

    ProtectedFieldRuntime.PreparedQuery prepareQuery(DynamicForm form,
                                                      DynamicForm physicalForm,
                                                      ConditionGroup where,
                                                      DataScope scope,
                                                      List<String> visibleFields) {
        return protectedFields.prepareQuery(
                form, physicalForm, FormDataScopes.unwrapTrustedValues(physicalForm, where, scope),
                scope, support.valueCodecs, visibleFields);
    }

    Optional<ProtectedFieldRuntime.PreparedContainsQuery> prepareContainsQuery(DynamicForm form,
                                                                                DynamicForm visibleForm,
                                                                                ConditionGroup where,
                                                                                DataScope scope) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        if (safeForm.protections().encryptedFields().isEmpty()) {
            return Optional.empty();
        }
        return protectedFields.prepareContainsQuery(
                safeForm, visibleForm, FormDataScopes.unwrapTrustedValues(safeForm, where, scope), scope,
                support.valueCodecs);
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

    SqlRequest selectLocking(ProtectedFieldRuntime.PreparedQuery query,
                             List<String> groups,
                             List<PageSort> sorts,
                             LockingReadDialect dialect,
                             ReadLock lock) {
        return queries.selectProjectedLocking(
                query.physicalForm(), query.where(), query.visibleFields(), groups, sorts, dialect, lock);
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

    SqlRequest selectKeyset(ProtectedFieldRuntime.PreparedQuery query,
                            HiddenProjectionLayout layout,
                            KeysetPageNormalizer.NormalizedKeysetPage page) {
        return queries.selectKeyset(query.physicalForm(), query.where(), layout, page);
    }

    SqlRequest selectKeysetLocking(ProtectedFieldRuntime.PreparedQuery query,
                                   HiddenProjectionLayout layout,
                                   KeysetPageNormalizer.NormalizedKeysetPage page,
                                   LockingReadDialect dialect,
                                   ReadLock lock) {
        return queries.selectKeysetLocking(
                query.physicalForm(), query.where(), layout, page, dialect, lock);
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

    Object maskValue(Object value,
                     com.flying.orm.core.protection.MaskedFieldDefinition definition,
                     SensitiveDisplayMode displayMode) {
        return protectedFields.maskValue(value, definition, displayMode);
    }

    DynamicForm physicalForm(DynamicForm form) {
        return protectedFields.physicalForm(form);
    }

}
