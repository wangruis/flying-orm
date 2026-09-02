package com.flying.orm.rdb.form.spec;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.execution.SqlExecutionOptions;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * 动态表单查询的不可变规格，统一承载条件、数据范围、投影、分组、排序和可选执行保护。
 *
 * <p>未显式提供执行保护时，由执行它的 FormClient 使用自身安全默认值。规格只保存结构与安全配置，
 * 不保存连接、订阅或响应式上下文，可在线程间安全复用；结构化条件的 JSON 形状值会在输入边界冻结，
 * 可信自定义标量则应由扩展方提供不可变对象。一次请求的条件值仍只进入参数绑定。</p>
 *
 * @author wangr
 * @date 2026-08-04
 * @version v1.0
 */
public final class QuerySpec {

    private final DynamicForm form;
    private final ConditionGroup where;
    private final DataScope scope;
    private final List<String> projections;
    private final List<String> groups;
    private final List<PageSort> sorts;
    private final SqlExecutionOptions executionOptions;
    private final StructuredConditionInput structuredInput;
    private final StructuredConditionPolicy structuredPolicy;
    private final SensitiveDisplayMode sensitiveDisplayMode;

    private QuerySpec(DynamicForm form,
                      ConditionGroup where,
                      DataScope scope,
                      List<String> projections,
                      List<String> groups,
                      List<PageSort> sorts,
                      SqlExecutionOptions executionOptions,
                      StructuredConditionInput structuredInput,
                      StructuredConditionPolicy structuredPolicy,
                      SensitiveDisplayMode sensitiveDisplayMode) {
        this.form = Objects.requireNonNull(form, "query form must not be null");
        this.where = Objects.requireNonNull(where, "query where must not be null");
        this.scope = Objects.requireNonNull(scope, "query data scope must not be null");
        this.projections = copyTextList(projections, "query projections");
        this.groups = copyTextList(groups, "query groups");
        this.sorts = List.copyOf(Objects.requireNonNull(sorts, "query sorts must not be null"));
        this.executionOptions = executionOptions;
        this.structuredInput = structuredInput;
        this.structuredPolicy = structuredPolicy;
        this.sensitiveDisplayMode = Objects.requireNonNull(
                sensitiveDisplayMode, "sensitive display mode must not be null");
    }

    /**
     * 创建使用客户端默认安全边界、无额外 DataScope 的普通条件查询。
     *
     * @param form 动态表单
     * @param where 参数化条件树
     * @return 查询规格
     */
    public static QuerySpec of(DynamicForm form, ConditionGroup where) {
        return new QuerySpec(form, where, DataScope.none(), List.of(), List.of(), List.of(), null, null, null,
                             SensitiveDisplayMode.DECLARED);
    }

    /**
     * 创建由安全结构化条件编译器处理的前端查询规格。
     *
     * @param form 动态表单
     * @param input 不可信前端条件输入
     * @return 使用默认结构限制策略的查询规格
     */
    public static QuerySpec structured(DynamicForm form, StructuredConditionInput input) {
        return new QuerySpec(form, ConditionGroup.and().build(), DataScope.none(), List.of(), List.of(), List.of(),
                             null, Objects.requireNonNull(input, "structured condition input must not be null"),
                             StructuredConditionPolicy.defaults(), SensitiveDisplayMode.DECLARED);
    }

    /** @return 查询使用的动态表单。 */
    public DynamicForm form() {
        return form;
    }

    /** @return 参数化条件树。 */
    public ConditionGroup where() {
        return where;
    }

    /** @return 本次查询附加的数据范围。 */
    public DataScope scope() {
        return scope;
    }

    /** @return 投影字段的不可变有序列表；空表示表单全部可读字段。 */
    public List<String> projections() {
        return projections;
    }

    /** @return 分组字段的不可变有序列表。 */
    public List<String> groups() {
        return groups;
    }

    /** @return 排序项的不可变有序列表。 */
    public List<PageSort> sorts() {
        return sorts;
    }

    /** @return 显式执行保护；为空时使用客户端默认保护。 */
    public Optional<SqlExecutionOptions> executionOptions() {
        return Optional.ofNullable(executionOptions);
    }

    /** @return 待安全编译的结构化条件；普通条件查询为空。 */
    public Optional<StructuredConditionInput> structuredInput() {
        return Optional.ofNullable(structuredInput);
    }

    /** @return 结构化条件限制策略；普通条件查询为空。 */
    public Optional<StructuredConditionPolicy> structuredPolicy() {
        return Optional.ofNullable(structuredPolicy);
    }

    /** @return 受保护业务结果的展示方式 */
    public SensitiveDisplayMode sensitiveDisplayMode() {
        return sensitiveDisplayMode;
    }

    /**
     * 返回仅替换数据范围的新规格。
     *
     * @param scope 不可为空的数据范围
     * @return 新查询规格
     */
    public QuerySpec withScope(DataScope scope) {
        return new QuerySpec(form, where, scope, projections, groups, sorts, executionOptions,
                             structuredInput, structuredPolicy, sensitiveDisplayMode);
    }

    /**
     * 返回仅替换投影与分组字段的新规格。
     *
     * @param projections 投影字段；不能为空列表
     * @param groups 分组字段
     * @return 新查询规格
     */
    public QuerySpec withProjection(List<String> projections, List<String> groups) {
        List<String> safeProjections = copyTextList(projections, "query projections");
        if (safeProjections.isEmpty()) {
            throw new IllegalArgumentException("projected query must select at least one field");
        }
        return new QuerySpec(form, where, scope, safeProjections, groups, sorts, executionOptions,
                             structuredInput, structuredPolicy, sensitiveDisplayMode);
    }

    /**
     * 返回仅替换排序的新规格。
     *
     * @param sorts 排序项
     * @return 新查询规格
     */
    public QuerySpec withSorts(List<PageSort> sorts) {
        return new QuerySpec(form, where, scope, projections, groups, sorts, executionOptions,
                             structuredInput, structuredPolicy, sensitiveDisplayMode);
    }

    /**
     * 返回使用显式执行保护的新规格。
     *
     * @param options 连接可用后的 SQL 超时、结果行数、结果内存和 LOB 边界
     * @return 新查询规格
     */
    public QuerySpec withExecutionOptions(SqlExecutionOptions options) {
        return new QuerySpec(form, where, scope, projections, groups, sorts,
                             Objects.requireNonNull(options, "query execution options must not be null"),
                             structuredInput, structuredPolicy, sensitiveDisplayMode);
    }

    /**
     * 返回仅替换结构化条件安全策略的新规格。
     *
     * @param policy 深度、节点数、字段、操作符和值边界
     * @return 新查询规格
     */
    public QuerySpec withStructuredPolicy(StructuredConditionPolicy policy) {
        if (structuredInput == null) {
            throw new IllegalStateException("structured policy requires a structured query spec");
        }
        return new QuerySpec(form, where, scope, projections, groups, sorts, executionOptions,
                             structuredInput, Objects.requireNonNull(policy, "structured policy must not be null"),
                             sensitiveDisplayMode);
    }

    /** 返回遵循字段注解或 DynamicForm 声明展示策略的新规格。 */
    public QuerySpec declaredDisplay() {
        return withSensitiveDisplayMode(SensitiveDisplayMode.DECLARED);
    }

    /** 返回对已声明脱敏字段强制脱敏的新规格。 */
    public QuerySpec masked() {
        return withSensitiveDisplayMode(SensitiveDisplayMode.MASKED);
    }

    /**
     * 返回向可信后端代码展示完整解密值的新规格。
     *
     * <p>该设置不会放宽 SQL 日志、异常或观测脱敏；上层仍负责调用授权。</p>
     */
    public QuerySpec showSensitive() {
        return withSensitiveDisplayMode(SensitiveDisplayMode.FULL);
    }

    private QuerySpec withSensitiveDisplayMode(SensitiveDisplayMode mode) {
        return new QuerySpec(form, where, scope, projections, groups, sorts, executionOptions,
                             structuredInput, structuredPolicy,
                             Objects.requireNonNull(mode, "sensitive display mode must not be null"));
    }

    private static List<String> copyTextList(List<String> values, String name) {
        List<String> copied = List.copyOf(Objects.requireNonNull(values, name + " must not be null"));
        if (copied.stream().anyMatch(value -> value == null || value.isBlank())) {
            throw new IllegalArgumentException(name + " must not contain blank values");
        }
        return copied;
    }
}
