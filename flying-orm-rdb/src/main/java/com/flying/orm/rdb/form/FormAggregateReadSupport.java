package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.result.DynamicRow;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 聚合 planner 对既有表单 Scope、逻辑删除、字段保护和 codec 的窄桥接。
 *
 * <p>这里不生成聚合 SQL，也不保存聚合状态；它只让 aggregate 包复用 form 包内已经存在的
 * 安全不变量，避免另建第二套 Scope 或保护实现。</p>
 *
 * @author wangr
 * @version v3.2
 */
@InternalApi
public final class FormAggregateReadSupport {

    private final FormDataSqlRenderer renderer;
    private final FormScopeSupport scopes;

    public FormAggregateReadSupport(FormDataSqlRenderer renderer,
                                    StructuredConditionResolver resolver,
                                    DataScope defaultDataScope) {
        this.renderer = Objects.requireNonNull(renderer, "form data sql renderer must not be null");
        this.scopes = new FormScopeSupport(
                this.renderer,
                Objects.requireNonNull(resolver, "structured condition resolver must not be null"),
                Objects.requireNonNull(defaultDataScope, "default data scope must not be null"));
    }

    /** 合并业务条件、客户端/调用 Scope、逻辑删除和受保护条件，且不获取连接。 */
    public PreparedRead prepare(QuerySpec spec) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "aggregate query spec must not be null");
        ScopedRead read = safeSpec.structuredInput()
                .map(input -> scopes.scopedStructuredRead(
                        safeSpec.form(), input,
                        safeSpec.structuredPolicy().orElse(StructuredConditionPolicy.defaults()),
                        safeSpec.scope()))
                .orElseGet(() -> scopes.scopedRead(
                        safeSpec.form(), safeSpec.where(), safeSpec.scope()));
        return prepare(safeSpec, read);
    }

    /**
     * governed 聚合额外保留同一次编译得到的业务条件，供字段用途和扩展能力审批复用。
     * Scope、租户和逻辑删除只存在于 {@link PreparedRead#where()}，不会被误记成调用方 FILTER。
     */
    public GovernedPreparedRead prepareGoverned(QuerySpec spec) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "aggregate query spec must not be null");
        FormScopeSupport.GovernedRead governed = safeSpec.structuredInput()
                .map(input -> scopes.governedStructuredRead(
                        safeSpec.form(), input,
                        safeSpec.structuredPolicy().orElse(StructuredConditionPolicy.defaults()),
                        safeSpec.scope()))
                .orElseGet(() -> scopes.governedRead(
                        safeSpec.form(), safeSpec.where(), safeSpec.scope()));
        return new GovernedPreparedRead(
                prepare(safeSpec, governed.read()), governed.businessWhere());
    }

    private PreparedRead prepare(QuerySpec spec, ScopedRead read) {
        ProtectedFieldRuntime.PreparedQuery prepared = renderer.protection().prepareQuery(
                spec.form(), read.form(), read.where(), read.scope());
        return new PreparedRead(
                spec.form(), read.form(), prepared.physicalForm(), prepared.where(), read.scope());
    }

    public String identifier(String value) {
        return renderer.conditionRenderer().identifier(value);
    }

    /** 物理关系使用与普通 CRUD 相同的 legacy/分段身份渲染规则。 */
    public String identifier(DynamicForm form) {
        return renderer.relationIdentifier(form);
    }

    /** SQL Server 的稳定数值聚合规则在装配时确定，规划请求时只读取这个不可变事实。 */
    @InternalApi
    public boolean sqlServerDialect() {
        return renderer.sqlServerDialect();
    }

    /** MIN/MAX 与结果别名排序复用普通查询的稳定 OFFSET_TIME 排序约束。 */
    @InternalApi
    public void requireStableOffsetTimeOrdering(DynamicField field) {
        renderer.requireStableOffsetTimeOrdering(field);
    }

    public SqlFragment renderWhere(DynamicForm physicalForm, ConditionGroup where) {
        return renderer.renderCondition(physicalForm, where);
    }

    /** 只在显式 governed 聚合遍历中审批扩展 term；默认聚合不调用。 */
    public void approveTermExtension(TermCondition term, FieldUse use) {
        FieldUseGuard.approveTermExtension(renderer, term, use);
    }

    /** HAVING 字段先按结果布局校验和编码，再映射到 planner 已声明的安全 SQL 表达式。 */
    public SqlFragment renderHaving(DynamicForm resultForm,
                                    ConditionGroup having,
                                    Map<String, String> expressionsByAlias) {
        return renderHaving(resultForm, having, expressionsByAlias, null, null);
    }

    /** 只在装配关系 term 时创建 HAVING 的外层限定表达式。 */
    public boolean hasCorrelatedTerms() {
        return renderer.conditionRenderer().hasCorrelatedTerms();
    }

    /** 内部聚合组合边界：关联表达式只从同一份已验证分组/聚合声明生成。 */
    @InternalApi
    public SqlFragment renderHaving(DynamicForm resultForm,
                                    ConditionGroup having,
                                    Map<String, String> expressionsByAlias,
                                    Map<String, String> correlatedExpressionsByAlias,
                                    String outerQualifier) {
        Map<String, String> safeExpressions = Map.copyOf(Objects.requireNonNull(
                expressionsByAlias, "aggregate alias expressions must not be null"));
        return renderer.renderCondition(
                resultForm,
                having,
                alias -> {
                    String expression = safeExpressions.get(FieldIdentity.of(alias).key());
                    if (expression == null) {
                        throw new IllegalArgumentException("HAVING references an undeclared aggregate result alias");
                    }
                    return expression;
                },
                correlatedExpressionsByAlias == null ? null
                        : alias -> correlatedExpressionsByAlias.get(FieldIdentity.of(alias).key()),
                correlatedExpressionsByAlias == null ? null : alias -> outerQualifier);
    }

    /** 普通字段及实体自定义字段沿用表单查询的同一解码规则。 */
    public Object decode(DynamicField field, Object value) {
        DynamicField safeField = Objects.requireNonNull(field, "aggregate source field must not be null");
        if (value == null) {
            return null;
        }
        EntityTypeMappingRegistry.Mapping custom = renderer.customFieldMapping(safeField);
        return custom == null
                ? renderer.readScalarValue(safeField, value)
                : custom.codec().read(value, custom.javaType());
    }

    /** MIN/MAX 在方言解码后继续走应用 codec，最终兑现表达式声明的 Java 类型。 */
    public Object decode(DynamicField field, Object value, Class<?> javaType) {
        Object decoded = decode(field, value);
        if (decoded == null) {
            return null;
        }
        Class<?> target = Objects.requireNonNull(javaType, "aggregate Java type must not be null");
        return target.isInstance(decoded)
                ? decoded : renderer.valueCodecs().read(decoded, target);
    }

    public Class<?> customJavaType(DynamicField field) {
        EntityTypeMappingRegistry.Mapping mapping = renderer.customFieldMapping(
                Objects.requireNonNull(field, "aggregate source field must not be null"));
        return mapping == null ? null : mapping.javaType();
    }

    /** 分组值仍遵循 QuerySpec 的声明/强制脱敏展示模式。 */
    public Object maskGroupValue(DynamicForm form,
                                 DynamicField field,
                                 Object value,
                                 SensitiveDisplayMode displayMode) {
        if (value == null || form.protections().masked(field.name()).isEmpty()) {
            return value;
        }
        Map<String, Object> single = new LinkedHashMap<>(1);
        single.put(field.name(), value);
        DynamicRow masked = renderer.protection().mask(
                form, DynamicRow.copyOf(single),
                Objects.requireNonNull(displayMode, "aggregate display mode must not be null"));
        return masked.get(field.name());
    }

    /** 一次规划共享的既有读取事实。 */
    public record PreparedRead(DynamicForm logicalForm,
                               DynamicForm readableForm,
                               DynamicForm physicalForm,
                               ConditionGroup where,
                               DataScope scope) {

        public PreparedRead {
            logicalForm = Objects.requireNonNull(logicalForm, "aggregate logical form must not be null");
            readableForm = Objects.requireNonNull(readableForm, "aggregate readable form must not be null");
            physicalForm = Objects.requireNonNull(physicalForm, "aggregate physical form must not be null");
            where = Objects.requireNonNull(where, "aggregate where must not be null");
            scope = Objects.requireNonNull(scope, "aggregate scope must not be null");
        }
    }

    /** 仅由 governed 聚合创建；既有 PreparedRead 的五组件布局保持不变。 */
    public record GovernedPreparedRead(PreparedRead read, ConditionGroup businessWhere) {

        public GovernedPreparedRead {
            read = Objects.requireNonNull(read, "governed aggregate read must not be null");
            businessWhere = Objects.requireNonNull(
                    businessWhere, "governed aggregate business where must not be null");
        }
    }
}
