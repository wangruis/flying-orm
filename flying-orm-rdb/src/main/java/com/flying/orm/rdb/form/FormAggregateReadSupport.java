package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionNode;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUseRequirements;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.core.sql.render.CorrelatedSqlTermHandler;
import com.flying.orm.core.metadata.Feature;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.internal.condition.ConditionNodes;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * 聚合读取的来源、Scope、保护条件和结果值准备，与普通读取直接共用表单内部所有者。
 *
 * @author wangr
 * @version v3.2
 */
final class FormAggregateReadSupport {

    final FormDataSqlRenderer renderer;
    private final FormScopeSupport scopes;

    FormAggregateReadSupport(FormDataSqlRenderer renderer, FormScopeSupport scopes) {
        this.renderer = renderer;
        this.scopes = scopes;
    }

    /** 合并业务条件、客户端/调用 Scope、逻辑删除和受保护条件，且不获取连接。 */
    PreparedRead prepare(QuerySpec spec) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "aggregate query spec must not be null");
        ScopedRead read = scopes.scopedRead(safeSpec);
        return prepare(safeSpec, read);
    }

    /**
     * governed 聚合额外保留同一次编译得到的业务条件，供字段用途和扩展能力审批复用。
     * Scope、租户和逻辑删除只存在于 {@link PreparedRead#where()}，不会被误记成调用方 FILTER。
     */
    GovernedPreparedRead prepareGoverned(QuerySpec spec) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "aggregate query spec must not be null");
        FormScopeSupport.GovernedRead governed = scopes.governedRead(safeSpec);
        return new GovernedPreparedRead(
                prepare(safeSpec, governed.read()), governed.businessWhere());
    }

    /** 关联报表复用同一来源规划，不创建普通 JOIN 的投影表单或解码计划。 */
    JoinedRead prepareJoin(JoinQuerySpec spec, FieldUseRequirements.Builder requirements,
                                  QueryShapeLimits limits, boolean governed) {
        JoinQueryPlanner planner = new JoinQueryPlanner(renderer, scopes, SqlExecutionOptions.unlimited());
        return new JoinedRead(spec, planner.prepareAggregateSources(
                spec, requirements, governed ? new QueryShapeBudget(limits) : null, governed));
    }

    /** 只供聚合规划消费；字段和 Scope 保留来源身份。 */
    final class JoinedRead {
        private final JoinQuerySpec spec;
        private final JoinQueryPlanner.PreparedSources sources;

        private JoinedRead(JoinQuerySpec spec, JoinQueryPlanner.PreparedSources sources) {
            this.spec = spec;
            this.sources = sources;
        }

        DynamicField field(JoinSource source, String field) {
            return sources.readableForms().get(source).field(field);
        }

        String expression(JoinSource source, String field) {
            return renderer.conditionRenderer().identifier("t" + source.ordinal())
                    + "." + renderer.conditionRenderer().identifier(field);
        }

        SqlFragment relation() {
            return renderer.joinQueries().relation(spec, sources.physicalForms(),
                    sources.protections(), sources.valueConditions());
        }

        FieldUseSnapshot approve(FieldUseRequirements requirements, FieldUsePolicy policy) {
            return JoinQueryPlanner.approveFieldUse(requirements, sources.scopes(), policy);
        }
    }

    private PreparedRead prepare(QuerySpec spec, ScopedRead read) {
        ProtectedFieldRuntime.PreparedQuery prepared = renderer.protection().prepareQuery(
                spec.form(), read.where(), read.scope(), List.of());
        return new PreparedRead(
                spec.form(), read.form(), prepared.physicalForm(), prepared.where(), read.scope());
    }

    /** 仅实际使用关系 term 的 HAVING 需要外层限定表达式；普通配置不遍历条件。 */
    boolean hasCorrelatedTerms(DynamicForm form, ConditionGroup condition) {
        if (form.features().features().isEmpty() && !renderer.conditionRenderer().hasCorrelatedTerms()) {
            return false;
        }
        for (ConditionNode node : ConditionNodes.preorder(condition)) {
            if (node instanceof TermCondition term) {
                Feature feature = form.features().find(term.operator()).orElse(null);
                if (feature instanceof SqlTermHandler
                        ? feature instanceof CorrelatedSqlTermHandler
                        : renderer.conditionRenderer().isCorrelatedTerm(term.operator())) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Alias SQL and value semantics come from the same already validated aggregate declaration. */
    SqlFragment renderHaving(DynamicForm resultForm,
                                    ConditionGroup having,
                                    Map<String, String> expressionsByAlias,
                                    Map<String, String> correlatedExpressionsByAlias,
                                    String outerQualifier,
                                    Function<String, DynamicField> valueFieldResolver) {
        return renderer.renderCondition(
                resultForm,
                having,
                alias -> {
                    String expression = expressionsByAlias.get(FieldIdentity.of(alias).key());
                    if (expression == null) {
                        throw new IllegalArgumentException("HAVING references an undeclared aggregate result alias");
                    }
                    return expression;
                },
                correlatedExpressionsByAlias == null ? null
                        : alias -> correlatedExpressionsByAlias.get(FieldIdentity.of(alias).key()),
                correlatedExpressionsByAlias == null ? null : alias -> outerQualifier,
                valueFieldResolver);
    }

    /** 一次准备字段读取规则；javaType 为 null 时保留字段类型，否则兑现聚合表达式的结果类型。 */
    UnaryOperator<Object> decoder(DynamicField field, Class<?> javaType) {
        DynamicField safeField = Objects.requireNonNull(field, "aggregate source field must not be null");
        EntityTypeMappingRegistry.Mapping custom = renderer.customFieldMapping(safeField);
        FormScalarReadPlan scalar = custom == null ? renderer.scalarReadPlan(safeField) : null;
        if (custom == null && scalar == null && javaType == null) return UnaryOperator.identity();
        return value -> {
            if (value == null) return null;
            Object decoded = custom != null ? custom.codec().read(value, custom.javaType())
                    : scalar != null ? scalar.read(value) : value;
            return decoded == null || javaType == null || javaType.isInstance(decoded)
                    ? decoded : renderer.valueCodecs().read(decoded, javaType);
        };
    }

    Class<?> customJavaType(DynamicField field) {
        EntityTypeMappingRegistry.Mapping mapping = renderer.customFieldMapping(
                Objects.requireNonNull(field, "aggregate source field must not be null"));
        return mapping == null ? null : mapping.javaType();
    }

    /** 分组值仍遵循 QuerySpec 的声明/强制脱敏展示模式。 */
    Object maskGroupValue(DynamicForm form,
                                 DynamicField field,
                                 Object value,
                                 SensitiveDisplayMode displayMode) {
        if (value == null) {
            return value;
        }
        MaskedFieldDefinition definition = form.protections().masked(field.name()).orElse(null);
        if (definition == null) {
            return value;
        }
        return renderer.protection().maskValue(value, definition,
                Objects.requireNonNull(displayMode, "aggregate display mode must not be null"));
    }

    /** 一次规划共享的既有读取事实。 */
    record PreparedRead(DynamicForm logicalForm,
                               DynamicForm readableForm,
                               DynamicForm physicalForm,
                               ConditionGroup where,
                               DataScope scope) {

        PreparedRead {
            logicalForm = Objects.requireNonNull(logicalForm, "aggregate logical form must not be null");
            readableForm = Objects.requireNonNull(readableForm, "aggregate readable form must not be null");
            physicalForm = Objects.requireNonNull(physicalForm, "aggregate physical form must not be null");
            where = Objects.requireNonNull(where, "aggregate where must not be null");
            scope = Objects.requireNonNull(scope, "aggregate scope must not be null");
        }
    }

    /** 显式字段治理额外消费业务条件，普通聚合不创建此状态。 */
    record GovernedPreparedRead(PreparedRead read, ConditionGroup businessWhere) {

        GovernedPreparedRead {
            read = Objects.requireNonNull(read, "governed aggregate read must not be null");
            businessWhere = Objects.requireNonNull(
                    businessWhere, "governed aggregate business where must not be null");
        }
    }
}
