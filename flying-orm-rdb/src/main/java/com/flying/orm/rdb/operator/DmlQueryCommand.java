package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionGroups;
import com.flying.orm.core.form.LogicDeleteDefinition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.scope.ScopeErrorCode;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlIdentifiers;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Consumer;

/**
 * 保存一条链式动态查询的可变构建状态，并生成最终不可变 SQL 请求。
 *
 * <p>这里没有 JDBC、R2DBC 或 Reactor 类型。响应式和同步门面共用本命令，因此投影校验、默认 Scope、
 * 显式 Scope、业务条件和逻辑删除永远按同一顺序合并。对象只供单次调用使用，不能跨线程共享。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
final class DmlQueryCommand {

    private final SqlRenderer renderer;
    private final DataScope defaultDataScope;
    private final List<String> projections = new ArrayList<>();

    private String table;
    private ConditionGroup where = ConditionGroup.and().build();
    private LogicDeleteDefinition logicDelete;
    private DataScope scope = DataScope.none();

    DmlQueryCommand(SqlRenderer renderer, DataScope defaultDataScope) {
        this.renderer = Objects.requireNonNull(renderer, "sql renderer must not be null");
        this.defaultDataScope = Objects.requireNonNull(defaultDataScope, "default data scope must not be null");
    }

    void select(String... columns) {
        Objects.requireNonNull(columns, "select columns must not be null");
        for (String column : columns) {
            projections.add(SqlIdentifiers.requireProjection(column, "select column"));
        }
    }

    void from(String table) {
        this.table = SqlIdentifiers.requireIdentifier(table, "query table");
    }

    void where(Consumer<WhereDsl> consumer) {
        Objects.requireNonNull(consumer, "where consumer must not be null");
        WhereDsl dsl = new WhereDsl(renderer);
        consumer.accept(dsl);
        this.where = dsl.build();
    }

    void logicDelete(String fieldName, Object notDeletedValue, Object deletedValue) {
        this.logicDelete = LogicDeleteDefinition.of(
                SqlIdentifiers.requireIdentifier(fieldName, "operator logic delete field"),
                notDeletedValue,
                deletedValue);
    }

    void scope(DataScope scope) {
        this.scope = this.scope.and(Objects.requireNonNull(scope, "data scope must not be null"));
    }

    /**
     * 在执行前一次完成标识符、字段权限和条件合并，失败时不会向数据库发送残缺 SQL。
     */
    SqlRequest toRequest() {
        String safeTable = SqlIdentifiers.requireIdentifier(table, "query table");
        DataScope effectiveScope = defaultDataScope.and(scope);
        StringBuilder sql = new StringBuilder("select ");
        sql.append(selectColumns(effectiveScope.fields(), safeTable));
        sql.append(" from ").append(renderer.identifier(safeTable));

        SqlFragment whereFragment = renderer.renderWhere(activeWhere(effectiveScope));
        if (!whereFragment.sql().isBlank()) {
            sql.append(" where ").append(whereFragment.sql());
        }
        return new SqlRequest(sql.toString(), whereFragment.parameters());
    }

    private String selectColumns(FieldScope fields, String resource) {
        FieldScope safeFields = Objects.requireNonNull(fields, "field scope must not be null");
        if (projections.isEmpty() && safeFields.unrestrictedRead()) {
            return "*";
        }
        StringJoiner joiner = new StringJoiner(", ");
        if (projections.isEmpty()) {
            safeFields.readableFields().stream().map(renderer::projection).forEach(joiner::add);
            if (joiner.length() == 0) {
                throw new ScopeAccessException(ScopeErrorCode.NO_READABLE_FIELDS,
                                               resource,
                                               null,
                                               "field scope leaves no readable fields for query");
            }
            return joiner.toString();
        }
        projections.stream()
                   .peek(projection -> requireReadableProjection(safeFields, projection, resource))
                   .map(renderer::projection)
                   .forEach(joiner::add);
        return joiner.toString();
    }

    private static void requireReadableProjection(FieldScope fields, String projection, String resource) {
        if (!fields.canRead(projection)) {
            throw new ScopeAccessException(ScopeErrorCode.FIELD_NOT_READABLE,
                                           resource,
                                           projection,
                                           "field [" + projection + "] is not readable for query");
        }
    }

    private ConditionGroup activeWhere(DataScope effectiveScope) {
        ConditionGroup active = effectiveScope.condition()
                                                  .map(scopeWhere -> ConditionGroups.and(where, scopeWhere))
                                                  .orElse(where);
        if (logicDelete == null) {
            return active;
        }
        ConditionGroup logicWhere = ConditionGroup.and()
                                                  .where(logicDelete.fieldName(),
                                                         "=",
                                                         logicDelete.notDeletedValue())
                                                  .build();
        return ConditionGroups.and(active, logicWhere);
    }
}
