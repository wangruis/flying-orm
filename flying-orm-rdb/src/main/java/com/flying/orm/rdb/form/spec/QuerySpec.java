package com.flying.orm.rdb.form.spec;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.ExpressionSort;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlIdentifiers;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.lock.ReadLock;
import com.flying.orm.rdb.internal.value.ExpressionFieldValue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.util.function.UnaryOperator;

/**
 * 查询的不可变规格，统一承载来源、条件、数据范围、投影、分组、排序、分页和可选执行保护。
 *
 * <p>未显式提供执行保护时，由执行它的 FormClient 使用自身安全默认值。规格只保存结构与安全配置，
 * 不保存连接、订阅或响应式上下文，可在线程间安全复用；结构化条件的 JSON 形状值会在输入边界冻结，
 * 可信自定义标量则应由扩展方提供不可变对象。一次请求的条件值仍只进入参数绑定；设置未改变时复用当前规格。</p>
 *
 * @author wangr
 * @date 2026-08-04
 * @version v1.0
 */
public final class QuerySpec {

    private final DynamicForm form;
    private final RelationIdentity relation;
    private final ConditionGroup where;
    private final DataScope scope;
    private final List<String> projections;
    private final List<String> groups;
    private final List<PageSort> sorts;
    private final SqlExecutionOptions executionOptions;
    private final StructuredConditionInput structuredInput;
    private final StructuredConditionPolicy structuredPolicy;
    private final SensitiveDisplayMode sensitiveDisplayMode;
    private final Map<String, ValueExpression> expressions;
    private final Set<String> excludedFields;
    private final ReadLock lock;
    private final PageQuery page;

    private QuerySpec(DynamicForm form, RelationIdentity relation,
                      ConditionGroup where,
                      DataScope scope,
                      List<String> projections,
                      List<String> groups,
                      List<PageSort> sorts,
                      SqlExecutionOptions executionOptions,
                      StructuredConditionInput structuredInput,
                      StructuredConditionPolicy structuredPolicy,
                      SensitiveDisplayMode sensitiveDisplayMode,
                      Map<String, ValueExpression> expressions,
                      Set<String> excludedFields, ReadLock lock, PageQuery page) {
        this.form = form;
        this.relation = form == null ? Objects.requireNonNull(relation, "query relation must not be null")
                : form.relationIdentity();
        this.where = where;
        this.scope = scope;
        this.projections = projections.isEmpty() || excludedFields.isEmpty() ? projections
                : projections.stream().filter(field -> !excludedFields.contains(FieldIdentity.of(field).key())).toList();
        if (!projections.isEmpty() && this.projections.isEmpty()) {
            throw new IllegalArgumentException("projected query must select at least one field");
        }
        this.groups = groups;
        this.sorts = sorts;
        this.executionOptions = executionOptions;
        this.structuredInput = structuredInput;
        this.structuredPolicy = structuredPolicy;
        this.sensitiveDisplayMode = sensitiveDisplayMode;
        if (!expressions.isEmpty() && !excludedFields.isEmpty()) {
            Map<String, ValueExpression> selected = new LinkedHashMap<>(expressions);
            for (String excluded : excludedFields) {
                if (sorts.stream().noneMatch(sort -> FieldIdentity.of(sort.field()).key().equals(excluded))) {
                    selected.remove(excluded);
                }
            }
            this.expressions = Map.copyOf(selected);
        } else {
            this.expressions = expressions;
        }
        this.excludedFields = excludedFields;
        this.lock = lock;
        this.page = page;
    }

    /**
     * 创建使用客户端默认安全边界、无额外 DataScope 的普通条件查询。
     *
     * @param form 动态表单
     * @param where 参数化条件树
     * @return 查询规格
     */
    public static QuerySpec of(DynamicForm form, ConditionGroup where) {
        DynamicForm safeForm = Objects.requireNonNull(form, "query form must not be null");
        ConditionGroup safeWhere = Objects.requireNonNull(where, "query where must not be null");
        return new QuerySpec(safeForm, null, safeWhere, DataScope.none(), List.of(), List.of(), List.of(),
                             null, null, null, SensitiveDisplayMode.DECLARED, Map.of(), Set.of(), null, null);
    }

    /** 组装查询状态，在 build 时一次发布不可变规格。 */
    public static Builder builder(DynamicForm form) {
        return new Builder(Objects.requireNonNull(form, "query form must not be null"));
    }

    /** 先选择字段后指定来源；冻结前必须调用 from。 */
    public static Builder builder() { return new Builder(null); }

    /** 无元数据的受信关系查询，不创建表单、不读取数据库元数据。 */
    public static Builder builder(RelationIdentity relation) { return builder().from(relation); }

    /** 接受 table、schema.table 或 catalog.schema.table 简写。 */
    public static Builder builder(String table) { return builder().from(table); }

    /**
     * 创建由安全结构化条件编译器处理的前端查询规格。
     *
     * @param form 动态表单
     * @param input 不可信前端条件输入
     * @return 使用默认结构限制策略的查询规格
     */
    public static QuerySpec structured(DynamicForm form, StructuredConditionInput input) {
        StructuredConditionInput safeInput = Objects.requireNonNull(
                input, "structured condition input must not be null");
        DynamicForm safeForm = Objects.requireNonNull(form, "query form must not be null");
        return new QuerySpec(safeForm, null, ConditionGroup.and().build(), DataScope.none(),
                             List.of(), List.of(), List.of(), null, safeInput,
                             StructuredConditionPolicy.defaults(), SensitiveDisplayMode.DECLARED, Map.of(), Set.of(), null, null);
    }

    /** @return 查询使用的动态表单。 */
    public DynamicForm form() {
        if (form == null) throw new IllegalStateException("query source has no dynamic form");
        return form;
    }

    public Optional<DynamicForm> model() { return Optional.ofNullable(form); }

    public RelationIdentity relationIdentity() {
        return relation;
    }

    public String table() {
        return relation.qualifiedName();
    }

    /** 普通读取、首行与 Keyset 共用的显式 SQL 行锁；计数不加锁，不创建或管理事务。 */
    public Optional<ReadLock> lock() { return Optional.ofNullable(lock); }

    /** 为根表设置 SQL 行锁，自动关联不扩大锁定目标；null 清除锁，其他查询状态保持不变。 */
    public QuerySpec withLock(ReadLock value) {
        return Objects.equals(lock, value) ? this : new QuerySpec(form, relation, where, scope,
                projections, groups, sorts, executionOptions, structuredInput, structuredPolicy,
                sensitiveDisplayMode, expressions, excludedFields, value, page);
    }

    /** 数据读取的可选窗口；first、count 和显式分页终端使用各自的窗口。 */
    public Optional<PageQuery> page() { return Optional.ofNullable(page); }

    /** 替换数据窗口；null 表示不分页。页内排序只在规格尚未指定排序时采用。 */
    public QuerySpec withPage(PageQuery value) {
        List<PageSort> ordered = value != null && sorts.isEmpty() ? value.sorts() : sorts;
        PageQuery window = value == null || value.sorts().isEmpty() ? value : PageQuery.of(value.page(), value.size());
        if (Objects.equals(page, window) && sorts.equals(ordered)) return this;
        return new QuerySpec(form, relation, where, scope, projections, groups, ordered, executionOptions,
                structuredInput, structuredPolicy, sensitiveDisplayMode, expressions, excludedFields, lock, window);
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

    /** @return 排除的大小写无关字段键；不影响条件、分组、排序或计数。 */
    public Set<String> excludedFields() {
        return excludedFields;
    }

    /** 追加排除字段或计算列；默认投影仍由执行时的字段范围裁窄，不转为显式选列。 */
    public QuerySpec withExcludedFields(Collection<String> fields) {
        Objects.requireNonNull(fields, "excluded fields must not be null");
        if (fields.isEmpty()) return this;
        Set<String> excluded = new HashSet<>(excludedFields);
        for (String field : fields) {
            excluded.add(FieldIdentity.of(SqlIdentifiers.requireIdentifier(field)).key());
        }
        return new QuerySpec(form, relation, where, scope, projections, groups, sorts, executionOptions,
                structuredInput, structuredPolicy, sensitiveDisplayMode, expressions, Set.copyOf(excluded), lock, page);
    }

    /** @return 按大小写无关名称索引的投影或排序表达式；值始终通过绑定参数发送。 */
    public Map<String, ValueExpression> expressions() {
        return expressions;
    }

    /** 追加一个计算结果列；可与 withProjection 声明的普通字段混合选择。 */
    public QuerySpec withExpression(String alias, ValueExpression expression) {
        Map<String, ValueExpression> selectedExpressions = new LinkedHashMap<>(expressions);
        List<String> selected = new ArrayList<>(projections);
        appendExpression(form, alias, expression, selectedExpressions, selected, true);
        return new QuerySpec(form, relation, where, scope, List.copyOf(selected), groups, sorts, executionOptions,
                structuredInput, structuredPolicy, sensitiveDisplayMode, Map.copyOf(selectedExpressions), excludedFields, lock, page);
    }

    /**
     * 按 Map 的迭代顺序追加计算列，一次冻结整组投影；有序输出可使用 LinkedHashMap。
     * 别名必须与现有字段及计算列不同，大小写不敏感；不保留调用方的可变 Map。
     */
    public QuerySpec withExpressions(Map<String, ? extends ValueExpression> additions) {
        Objects.requireNonNull(additions, "projection expressions must not be null");
        if (additions.isEmpty()) return this;
        Map<String, ValueExpression> selectedExpressions = new LinkedHashMap<>(expressions);
        List<String> selected = new ArrayList<>(projections);
        for (Map.Entry<String, ? extends ValueExpression> entry : additions.entrySet()) {
            appendExpression(form, entry.getKey(), entry.getValue(), selectedExpressions, selected, true);
        }
        return new QuerySpec(form, relation, where, scope, List.copyOf(selected), groups, sorts, executionOptions,
                structuredInput, structuredPolicy, sensitiveDisplayMode, Map.copyOf(selectedExpressions), excludedFields, lock, page);
    }

    private static void appendExpression(DynamicForm form, String alias, ValueExpression expression,
                                   Map<String, ValueExpression> selectedExpressions, List<String> selected, boolean bind) {
        String name = SqlIdentifiers.requireIdentifier(alias);
        String key = FieldIdentity.of(name).key();
        if (name.indexOf('.') >= 0 || form != null && form.findField(name).isPresent() || selectedExpressions.containsKey(key)) {
            throw new IllegalArgumentException("expression alias must be an unused, unqualified result name");
        }
        ValueExpression value = Objects.requireNonNull(expression, "projection expression must not be null");
        selectedExpressions.put(key, bind ? bindPriorityValues(form, value) : value);
        selected.add(name);
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
     * 返回仅替换数据范围的规格。
     *
     * @param scope 不可为空的数据范围
     * @return 查询规格
     */
    public QuerySpec withScope(DataScope scope) {
        return this.scope == scope ? this : new QuerySpec(form, relation, where, Objects.requireNonNull(scope, "query data scope must not be null"),
                             projections, groups, sorts, executionOptions,
                             structuredInput, structuredPolicy, sensitiveDisplayMode, expressions, excludedFields, lock, page);
    }

    /**
     * 返回仅替换投影与分组字段的规格。
     *
     * @param projections 普通字段或已声明的计算列别名；不能为空列表，未选中且未用于排序的计算列被移除
     * @param groups 分组字段
     * @return 查询规格
     */
    public QuerySpec withProjection(List<String> projections, List<String> groups) {
        List<String> safeProjections = copyTextList(projections, "query projections");
        if (safeProjections.isEmpty()) {
            throw new IllegalArgumentException("projected query must select at least one field");
        }
        List<String> safeGroups = copyTextList(groups, "query groups");
        Map<String, ValueExpression> selectedExpressions = selectedExpressions(expressions, safeProjections, sorts);
        return new QuerySpec(form, relation, where, scope, safeProjections, safeGroups, sorts, executionOptions,
                             structuredInput, structuredPolicy, sensitiveDisplayMode, selectedExpressions, excludedFields, lock, page);
    }

    private static Map<String, ValueExpression> selectedExpressions(Map<String, ValueExpression> expressions,
                                                                   List<String> projections, List<PageSort> sorts) {
        if (!expressions.isEmpty()) {
            Map<String, ValueExpression> selectedExpressions = new LinkedHashMap<>();
            for (String field : projections) {
                String key = FieldIdentity.of(field).key();
                ValueExpression expression = expressions.get(key);
                if (expression != null) selectedExpressions.put(key, expression);
            }
            for (PageSort sort : sorts) {
                String key = FieldIdentity.of(sort.field()).key();
                ValueExpression expression = expressions.get(key);
                if (expression != null) selectedExpressions.put(key, expression);
            }
            return Map.copyOf(selectedExpressions);
        }
        return Map.of();
    }

    /**
     * 返回仅替换排序的规格。
     *
     * @param sorts 排序项
     * @return 查询规格
     */
    public QuerySpec withSorts(List<PageSort> sorts) {
        if (this.sorts.equals(sorts)) return this;
        List<PageSort> safeSorts = List.copyOf(
                Objects.requireNonNull(sorts, "query sorts must not be null"));
        return new QuerySpec(form, relation, where, scope, projections, groups, safeSorts, executionOptions,
                             structuredInput, structuredPolicy, sensitiveDisplayMode, expressions, excludedFields, lock, page);
    }

    /**
     * 返回使用显式执行保护的规格。
     *
     * @param options 结果行数、结果内存和 LOB 边界
     * @return 查询规格
     */
    public QuerySpec withExecutionOptions(SqlExecutionOptions options) {
        if (options != null && executionOptions == options) return this;
        return new QuerySpec(form, relation, where, scope, projections, groups, sorts,
                             Objects.requireNonNull(options, "query execution options must not be null"),
                             structuredInput, structuredPolicy, sensitiveDisplayMode, expressions, excludedFields, lock, page);
    }

    /**
     * 返回仅替换结构化条件安全策略的规格。
     *
     * @param policy 深度、节点数、字段、操作符和值边界
     * @return 查询规格
     */
    public QuerySpec withStructuredPolicy(StructuredConditionPolicy policy) {
        if (structuredInput == null) {
            throw new IllegalStateException("structured policy requires a structured query spec");
        }
        if (structuredPolicy == policy) return this;
        return new QuerySpec(form, relation, where, scope, projections, groups, sorts, executionOptions,
                             structuredInput, Objects.requireNonNull(policy, "structured policy must not be null"),
                             sensitiveDisplayMode, expressions, excludedFields, lock, page);
    }

    /** 返回遵循字段注解或 DynamicForm 声明展示策略的规格。 */
    public QuerySpec declaredDisplay() {
        return withSensitiveDisplayMode(SensitiveDisplayMode.DECLARED);
    }

    /** 返回对已声明脱敏字段强制脱敏的规格。 */
    public QuerySpec masked() {
        return withSensitiveDisplayMode(SensitiveDisplayMode.MASKED);
    }

    /**
     * 返回向可信后端代码展示完整解密值的规格。
     *
     * <p>该设置不会放宽 SQL 日志、异常或观测脱敏；上层仍负责调用授权。</p>
     */
    public QuerySpec showSensitive() {
        return withSensitiveDisplayMode(SensitiveDisplayMode.FULL);
    }

    private QuerySpec withSensitiveDisplayMode(SensitiveDisplayMode mode) {
        return sensitiveDisplayMode == mode ? this : new QuerySpec(form, relation, where, scope, projections, groups, sorts, executionOptions,
                             structuredInput, structuredPolicy, mode, expressions, excludedFields, lock, page);
    }

    /** 追加仅用于排序的表达式，不改变返回列；调用顺序即排序优先级。 */
    public QuerySpec withOrderBy(ValueExpression expression, PageSort.Direction direction) {
        return withOrderBy(List.of(new ExpressionSort(expression, direction)));
    }

    /** 按迭代顺序追加整组排序，一次冻结列表与表达式；普通字段可引用已选择的计算别名。 */
    public QuerySpec withOrderBy(List<ExpressionSort> additions) {
        Objects.requireNonNull(additions, "sort expressions must not be null");
        if (additions.isEmpty()) return this;
        Map<String, ValueExpression> combined = expressions;
        List<PageSort> ordered = new ArrayList<>(sorts);
        combined = appendOrderBy(form, additions, combined, ordered, UnaryOperator.identity());
        return new QuerySpec(form, relation, where, scope, projections, groups, List.copyOf(ordered), executionOptions,
                structuredInput, structuredPolicy, sensitiveDisplayMode, Map.copyOf(combined), excludedFields, lock, page);
    }

    private static Map<String, ValueExpression> appendOrderBy(DynamicForm form, List<ExpressionSort> additions,
                                      Map<String, ValueExpression> combined, List<PageSort> ordered,
                                      UnaryOperator<String> fields) {
        int index = ordered.size();
        boolean copied = false;
        for (ExpressionSort sort : additions) {
            String name;
            if (sort.expression() instanceof ValueExpression.Field field && field.source() == null) {
                name = field.identity().name();
                if (!combined.containsKey(field.identity().key())) name = fields.apply(name);
            } else {
                if (!copied) {
                    combined = new LinkedHashMap<>(combined);
                    copied = true;
                }
                do {
                    name = "__fo_order_" + index++;
                } while (form != null && form.findField(name).isPresent() || combined.containsKey(name));
                combined.put(name, bindPriorityValues(form, sort.expression()));
            }
            ordered.add(new PageSort(name, sort.direction()));
        }
        return combined;
    }

    /** 替换服务端业务条件；存在前端结构化条件时，两者仍以 AND 合并。 */
    public QuerySpec withWhere(ConditionGroup where) {
        return this.where == where ? this : new QuerySpec(form, relation, Objects.requireNonNull(where, "query where must not be null"),
                             scope, projections, groups, sorts, executionOptions,
                             structuredInput, structuredPolicy, sensitiveDisplayMode, expressions, excludedFields, lock, page);
    }

    private static List<String> copyTextList(List<String> values, String name) {
        List<String> copied = List.copyOf(Objects.requireNonNull(values, name + " must not be null"));
        for (String value : copied) {
            if (value.isBlank()) {
                throw new IllegalArgumentException(name + " must not contain blank values");
            }
        }
        return copied;
    }

    private static ValueExpression bindPriorityValues(DynamicForm form, ValueExpression expression) {
        if (form == null) return expression;
        return ExpressionFieldValue.bindPriorities(expression, field -> field.source() == null
                ? form.findField(field.identity().name()).orElse(null) : null);
    }

    /** 单次查询的可变构建器；构建后继续修改不会改变已发布规格。 */
    public static final class Builder {
        private DynamicForm form;
        private RelationIdentity relation;
        private ConditionGroup where = ConditionGroup.and().build();
        private DataScope scope = DataScope.none();
        private List<String> projections = List.of();
        private List<String> groups = List.of();
        private List<ExpressionSort> sorts = List.of();
        private Map<String, ValueExpression> expressions = Map.of();
        private Set<String> exclusions = Set.of();
        private SqlExecutionOptions executionOptions;
        private ReadLock lock;
        private PageQuery page;
        private StructuredConditionInput structuredInput;
        private StructuredConditionPolicy structuredPolicy;
        private SensitiveDisplayMode display = SensitiveDisplayMode.DECLARED;

        private Builder(DynamicForm form) { this.form = form; }

        /** 复制可继续修改的查询分支；条件和不可变表达式可共享，集合状态互不影响。 */
        public Builder copy() {
            Builder copy = new Builder(form);
            copy.relation = relation;
            copy.where = where;
            copy.scope = scope;
            copy.projections = List.copyOf(projections);
            copy.groups = List.copyOf(groups);
            copy.sorts = List.copyOf(sorts);
            copy.expressions = Map.copyOf(expressions);
            copy.exclusions = Set.copyOf(exclusions);
            copy.executionOptions = executionOptions;
            copy.lock = lock;
            copy.page = page;
            copy.structuredInput = structuredInput;
            copy.structuredPolicy = structuredPolicy;
            copy.display = display;
            return copy;
        }

        public boolean hasProjection() { return !projections.isEmpty(); }
        public boolean hasGroups() { return !groups.isEmpty(); }
        public boolean hasExpressions() { return !expressions.isEmpty(); }
        public boolean hasExclusions() { return !exclusions.isEmpty(); }
        public boolean hasModel() { return form != null; }
        public DataScope scope() { return scope; }
        public Optional<PageQuery> page() { return Optional.ofNullable(page); }

        public Builder from(DynamicForm value) {
            form = Objects.requireNonNull(value, "query form must not be null");
            relation = null;
            return this;
        }

        public Builder from(RelationIdentity value) {
            relation = Objects.requireNonNull(value, "query relation must not be null");
            form = null;
            return this;
        }

        public Builder from(String value) {
            return from(RelationIdentity.parse(SqlIdentifiers.requireIdentifier(value)));
        }

        public Builder where(ConditionGroup value) {
            where = Objects.requireNonNull(value, "query where must not be null");
            return this;
        }

        public Builder scope(DataScope value) {
            scope = Objects.requireNonNull(value, "query data scope must not be null");
            return this;
        }

        public Builder select(String... fields) {
            if (fields.length == 0) return this;
            projections = mutable(projections);
            for (String field : fields) projections.add(SqlIdentifiers.requireProjection(field));
            return this;
        }

        public Builder groupBy(String... fields) {
            if (fields.length == 0) return this;
            groups = mutable(groups);
            for (String field : fields) groups.add(SqlIdentifiers.requireIdentifier(field));
            return this;
        }

        public Builder projection(List<String> selected, List<String> grouped) {
            List<String> safeSelected = copyTextList(selected, "query projections");
            List<String> safeGrouped = copyTextList(grouped, "query groups");
            if (safeSelected.isEmpty()) throw new IllegalArgumentException("projected query must select at least one field");
            projections = safeSelected;
            groups = safeGrouped;
            return this;
        }

        public Builder expressions(Map<String, ? extends ValueExpression> values) {
            Objects.requireNonNull(values, "projection expressions must not be null");
            values.forEach(this::expression);
            return this;
        }

        public Builder expression(String alias, ValueExpression value) {
            if (!(expressions instanceof LinkedHashMap)) expressions = new LinkedHashMap<>(expressions);
            projections = mutable(projections);
            appendExpression(form, alias, value, expressions, projections, false);
            return this;
        }

        public Builder orderBy(List<ExpressionSort> values) {
            Objects.requireNonNull(values, "sort expressions must not be null");
            if (values.isEmpty()) return this;
            sorts = mutable(sorts);
            sorts.addAll(List.copyOf(values));
            return this;
        }

        public Builder orderBy(ValueExpression value, PageSort.Direction direction) {
            sorts = mutable(sorts);
            sorts.add(new ExpressionSort(value, direction));
            return this;
        }

        public Builder sort(PageSort... values) {
            for (PageSort value : values) orderBy(ValueExpression.field(value.field()), value.direction());
            return this;
        }

        public Builder exclude(String... fields) { return exclude(java.util.Arrays.asList(fields)); }

        public Builder exclude(Collection<String> fields) {
            Objects.requireNonNull(fields, "excluded fields must not be null");
            if (fields.isEmpty()) return this;
            if (!(exclusions instanceof HashSet)) exclusions = new HashSet<>(exclusions);
            for (String field : fields) {
                exclusions.add(FieldIdentity.of(SqlIdentifiers.requireIdentifier(field)).key());
            }
            return this;
        }

        public Builder filter(StructuredConditionInput input, StructuredConditionPolicy policy) {
            structuredInput = Objects.requireNonNull(input, "structured condition input must not be null");
            structuredPolicy = Objects.requireNonNull(policy, "structured condition policy must not be null");
            return this;
        }

        /** null 清除显式选项，恢复使用客户端默认值。 */
        public Builder executionOptions(SqlExecutionOptions value) {
            executionOptions = value;
            return this;
        }

        public Builder display(SensitiveDisplayMode value) {
            display = Objects.requireNonNull(value, "sensitive display mode must not be null");
            return this;
        }

        /** 设置显式 SQL 行锁；null 恢复普通读取。 */
        public Builder lock(ReadLock value) {
            lock = value;
            return this;
        }

        /** 导入完整查询输入，保留服务端 where、Scope、分组和已声明计算列。 */
        public Builder input(QueryInput value) {
            return input(value, StructuredConditionPolicy.defaults());
        }

        public Builder input(QueryInput value, StructuredConditionPolicy policy) {
            Objects.requireNonNull(value, "query input must not be null");
            projections = copyTextList(value.select(), "query projections");
            exclusions = Set.of();
            exclude(value.exclude());
            sorts = List.of();
            sort(value.sorts().toArray(PageSort[]::new));
            page(value.page());
            structuredInput = value.filter();
            structuredPolicy = structuredInput == null ? null
                    : Objects.requireNonNull(policy, "structured condition policy must not be null");
            return this;
        }

        /** 页内排序只作为默认排序；窗口本身不重复保存排序状态。 */
        public Builder page(PageQuery value) {
            if (value != null && sorts.isEmpty()) sort(value.sorts().toArray(PageSort[]::new));
            page = value == null || value.sorts().isEmpty() ? value : PageQuery.of(value.page(), value.size());
            return this;
        }

        public QuerySpec build() {
            return build(UnaryOperator.identity());
        }

        /** 冻结时映射普通排序字段；计算列别名保留，不受声明先后顺序影响。 */
        public QuerySpec build(UnaryOperator<String> sortFields) {
            requireSource();
            List<PageSort> ordered = sorts.isEmpty() ? List.of() : new ArrayList<>(sorts.size());
            Map<String, ValueExpression> combined = expressions;
            if (form != null && !expressions.isEmpty()) {
                combined = new LinkedHashMap<>();
                for (Map.Entry<String, ValueExpression> entry : expressions.entrySet()) {
                    if (form.findField(entry.getKey()).isPresent()) {
                        throw new IllegalArgumentException("expression alias must be an unused, unqualified result name");
                    }
                    combined.put(entry.getKey(), bindPriorityValues(form, entry.getValue()));
                }
            }
            combined = appendOrderBy(form, sorts, combined, ordered,
                    Objects.requireNonNull(sortFields, "sort field mapping must not be null"));
            return new QuerySpec(form, relation, where, scope, List.copyOf(projections), List.copyOf(groups), List.copyOf(ordered),
                    executionOptions, structuredInput, structuredPolicy, display,
                    selectedExpressions(combined, projections, ordered), Set.copyOf(exclusions), lock, page);
        }

        /** 计数沿用分组与条件，不执行读取投影、排序或排除。 */
        public QuerySpec buildCount() {
            requireSource();
            List<String> grouped = List.copyOf(groups);
            return new QuerySpec(form, relation, where, scope, grouped, grouped, List.of(), executionOptions,
                    structuredInput, structuredPolicy, display, Map.of(), Set.of(), null, null);
        }

        /** 独立聚合规划只继承条件、范围、输入策略与执行选项。 */
        public QuerySpec buildConditions() {
            requireSource();
            return new QuerySpec(form, relation, where, scope, List.of(), List.of(), List.of(), executionOptions,
                    structuredInput, structuredPolicy, display, Map.of(), Set.of(), null, null);
        }

        private void requireSource() {
            if (form == null && relation == null) {
                throw new IllegalStateException("query source must be configured");
            }
            if (form == null && (structuredInput != null || display != SensitiveDisplayMode.DECLARED
                    || !expressions.isEmpty() || sorts.stream().anyMatch(sort ->
                    !(sort.expression() instanceof ValueExpression.Field field) || field.source() != null))) {
                throw new IllegalStateException("frontend filters, computed expressions and field protection require from(DynamicForm)");
            }
        }

        private static <T> List<T> mutable(List<T> values) {
            return values instanceof ArrayList<?> ? values : new ArrayList<>(values);
        }
    }
}
