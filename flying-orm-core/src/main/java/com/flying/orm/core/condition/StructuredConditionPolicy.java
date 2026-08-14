package com.flying.orm.core.condition;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * 前端条件白名单策略，把“外部可以表达什么”和“内部真正执行什么”明确分开。
 *
 * <p>实例不可变。每个 {@code allow/deny/with} 方法都会返回一个新策略，因此可以安全地把基础策略
 * 作为单例复用，再按表单或接口追加限制。拒绝名单优先于允许名单，字段专属 operator 白名单又会
 * 进一步收窄全局 operator。</p>
 *
 * @author wangr
 * @date 2026-07-24
 * @version v1.0
 */
public final class StructuredConditionPolicy {

    private static final int DEFAULT_MAX_DEPTH = 8;

    /** 条件编译阶段会按树结构递归，硬上限防止错误配置把安全阈值放大成线程栈风险。 */
    private static final int MAX_DEPTH_LIMIT = 64;

    private static final int DEFAULT_MAX_NODES = 100;

    private static final int MAX_NODES_LIMIT = 10_000;

    private static final int DEFAULT_MAX_COLLECTION_SIZE = 1_000;

    private static final int MAX_COLLECTION_SIZE_LIMIT = 1_000;

    private static final int DEFAULT_MAX_STRING_LENGTH = 4_096;

    private final Map<String, String> operators;

    private final Set<String> allowedFields;

    /** false 表示调用方从未设置字段白名单；true 时空集合明确表示一个字段都不允许。 */
    private final boolean allowedFieldsRestricted;

    private final Set<String> deniedFields;

    private final Map<String, Set<String>> fieldOperators;

    private final int maxDepth;

    private final int maxNodes;

    private final int maxCollectionSize;

    private final int maxStringLength;

    private final TermRegistry terms;

    private StructuredConditionPolicy(Map<String, String> operators,
                                      Set<String> allowedFields,
                                      boolean allowedFieldsRestricted,
                                      Set<String> deniedFields,
                                      Map<String, Set<String>> fieldOperators,
                                      int maxDepth,
                                      int maxNodes,
                                      int maxCollectionSize,
                                      int maxStringLength,
                                      TermRegistry terms) {
        this.operators = Map.copyOf(operators);
        this.allowedFields = Set.copyOf(allowedFields);
        this.allowedFieldsRestricted = allowedFieldsRestricted;
        this.deniedFields = Set.copyOf(deniedFields);
        this.fieldOperators = copyFieldOperators(fieldOperators);
        this.maxDepth = requireDepth(maxDepth);
        this.maxNodes = requireAtMost(maxNodes, MAX_NODES_LIMIT, "structured condition max nodes");
        this.maxCollectionSize = requireAtMost(
                maxCollectionSize, MAX_COLLECTION_SIZE_LIMIT, "structured condition max collection size");
        this.maxStringLength = requirePositive(maxStringLength, "structured condition max string length");
        this.terms = Objects.requireNonNull(terms, "term registry must not be null");
    }

    /**
     * 默认只开放常用参数化条件，自定义条件需要调用方显式放行。
     *
     * @return 默认策略
     */
    public static StructuredConditionPolicy defaults() {
        return new StructuredConditionPolicy(defaultOperators(),
                                             Set.of(),
                                             false,
                                             Set.of(),
                                             Map.of(),
                                             DEFAULT_MAX_DEPTH,
                                             DEFAULT_MAX_NODES,
                                             DEFAULT_MAX_COLLECTION_SIZE,
                                             DEFAULT_MAX_STRING_LENGTH,
                                             TermRegistry.empty());
    }

    /**
     * 放行一个外部操作符，内部也使用同名 term id。
     *
     * @param operator 操作符
     * @return 新策略
     */
    public StructuredConditionPolicy allowOperator(String operator) {
        return allowOperator(operator, operator);
    }

    /**
     * 放行一个外部操作符，并映射到内部 term id。
     *
     * @param externalOperator 前端传来的操作符
     * @param internalOperator 内部 term id
     * @return 新策略
     */
    public StructuredConditionPolicy allowOperator(String externalOperator, String internalOperator) {
        Map<String, String> copiedOperators = new LinkedHashMap<>(operators);
        copiedOperators.put(ConditionNames.normalize(externalOperator, "external condition operator"),
                            ConditionNames.normalize(internalOperator, "internal condition operator"));
        return copy(copiedOperators,
                    allowedFields,
                    deniedFields,
                    fieldOperators,
                    maxDepth,
                    maxNodes,
                    maxCollectionSize,
                    maxStringLength);
    }

    /**
     * 限定前端只能查这些字段；不调用时默认使用表单里的全部字段。
     *
     * @param fields 允许查询的字段
     * @return 新策略
     */
    public StructuredConditionPolicy allowOnlyFields(Collection<String> fields) {
        Objects.requireNonNull(fields, "structured condition allowed fields must not be null");
        Set<String> copiedFields = new LinkedHashSet<>();
        for (String field : fields) {
            copiedFields.add(ConditionNames.normalize(field, "structured condition allowed field"));
        }
        return new StructuredConditionPolicy(operators,
                                             copiedFields,
                                             true,
                                             deniedFields,
                                             fieldOperators,
                                             maxDepth,
                                             maxNodes,
                                             maxCollectionSize,
                                             maxStringLength,
                                             terms);
    }

    /**
     * 明确拒绝前端使用这些字段。即使其他地方把字段放进白名单，这里仍然优先拒绝。
     *
     * @param fields 不允许由前端传入条件的字段
     * @return 新策略
     */
    public StructuredConditionPolicy denyFields(Collection<String> fields) {
        Objects.requireNonNull(fields, "structured condition denied fields must not be null");
        Set<String> copiedFields = new LinkedHashSet<>(deniedFields);
        for (String field : fields) {
            copiedFields.add(ConditionNames.normalize(field, "structured condition denied field"));
        }
        return copy(operators,
                    allowedFields,
                    copiedFields,
                    fieldOperators,
                    maxDepth,
                    maxNodes,
                    maxCollectionSize,
                    maxStringLength);
    }

    /**
     * 给某个字段限定可用操作符。只要配置过，其他操作符就不能再用在这个字段上。
     *
     * @param field     字段名
     * @param operators 这个字段允许的前端操作符
     * @return 新策略
     */
    public StructuredConditionPolicy allowFieldOperators(String field, Collection<String> operators) {
        Objects.requireNonNull(operators, "structured condition field operators must not be null");
        String normalizedField = ConditionNames.normalize(field, "structured condition field operator field");
        Set<String> normalizedOperators = new LinkedHashSet<>();
        for (String operator : operators) {
            normalizedOperators.add(ConditionNames.normalize(operator, "structured condition field operator"));
        }
        if (normalizedOperators.isEmpty()) {
            throw new IllegalArgumentException("structured condition field operators must not be empty");
        }
        Map<String, Set<String>> copiedFieldOperators = new LinkedHashMap<>(fieldOperators);
        copiedFieldOperators.put(normalizedField, normalizedOperators);
        return copy(this.operators,
                    allowedFields,
                    deniedFields,
                    copiedFieldOperators,
                    maxDepth,
                    maxNodes,
                    maxCollectionSize,
                    maxStringLength);
    }

    /**
     * 设置条件树最大深度。为了保证递归编译不会耗尽线程栈，最大只能设为 64。
     *
     * @param maxDepth 最大深度
     * @return 新策略
     */
    public StructuredConditionPolicy withMaxDepth(int maxDepth) {
        return copy(operators, allowedFields, deniedFields, fieldOperators, maxDepth, maxNodes, maxCollectionSize, maxStringLength);
    }

    /**
     * 设置最多允许多少个条件节点。
     *
     * @param maxNodes 最大节点数
     * @return 新策略
     */
    public StructuredConditionPolicy withMaxNodes(int maxNodes) {
        return copy(operators, allowedFields, deniedFields, fieldOperators, maxDepth, maxNodes, maxCollectionSize, maxStringLength);
    }

    /**
     * 设置 in 或自定义集合值的最大元素数。
     *
     * @param maxCollectionSize 最大集合大小
     * @return 新策略
     */
    public StructuredConditionPolicy withMaxCollectionSize(int maxCollectionSize) {
        return copy(operators, allowedFields, deniedFields, fieldOperators, maxDepth, maxNodes, maxCollectionSize, maxStringLength);
    }

    /**
     * 设置单个字符串值最大长度。
     *
     * @param maxStringLength 最大字符串长度
     * @return 新策略
     */
    public StructuredConditionPolicy withMaxStringLength(int maxStringLength) {
        return copy(operators, allowedFields, deniedFields, fieldOperators, maxDepth, maxNodes, maxCollectionSize, maxStringLength);
    }

    /**
     * 给前端允许使用的自定义 term 声明值形状。集合、区间、无值以及可接收单值或集合的 term
     * 必须在这里明确注册；没有注册的自定义 term 只按单值处理。
     */
    public StructuredConditionPolicy withTerms(TermRegistry terms) {
        return new StructuredConditionPolicy(operators,
                                             allowedFields,
                                             allowedFieldsRestricted,
                                             deniedFields,
                                             fieldOperators,
                                             maxDepth,
                                             maxNodes,
                                             maxCollectionSize,
                                             maxStringLength,
                                             terms);
    }

    Optional<String> resolveOperator(String externalOperator) {
        return Optional.ofNullable(operators.get(ConditionNames.normalize(externalOperator, "condition operator")));
    }

    boolean allowsField(String field) {
        String normalizedField = ConditionNames.normalize(field, "condition field");
        return !deniedFields.contains(normalizedField)
                && (!allowedFieldsRestricted || allowedFields.contains(normalizedField));
    }

    boolean allowsFieldOperator(String field, String externalOperator) {
        Set<String> allowedOperators = fieldOperators.get(ConditionNames.normalize(field, "condition field"));
        return allowedOperators == null
                || allowedOperators.contains(ConditionNames.normalize(externalOperator, "condition operator"));
    }

    int maxDepth() {
        return maxDepth;
    }

    int maxNodes() {
        return maxNodes;
    }

    int maxCollectionSize() {
        return maxCollectionSize;
    }

    int maxStringLength() {
        return maxStringLength;
    }

    /**
     * 查找内部 term 声明的值形状。标准 term 的形状是框架安全契约，不能被业务注册表覆盖；
     * 自定义 term 再从当前策略注册表查找。没有元数据的自定义 term 只能接收单值。
     */
    ConditionValueShape valueShape(String internalOperator) {
        String operator = ConditionNames.normalize(internalOperator, "condition operator");
        return TermRegistry.standard()
                    .find(operator)
                    .or(() -> terms.find(operator))
                    .map(TermHandler::shape)
                    .orElse(ConditionValueShape.SCALAR);
    }

    /**
     * 标准比较条件的值表示字段值，需要按字段类型转换。自定义 term 的值通常表示另一个领域对象，
     * 例如 user-in-org 接收机构 ID，不能错误地按 userId 字段类型转换。
     */
    boolean usesFieldValue(String internalOperator) {
        String operator = ConditionNames.normalize(internalOperator, "condition operator");
        return TermRegistry.standard().find(operator).isPresent();
    }

    private StructuredConditionPolicy copy(Map<String, String> operators,
                                           Set<String> allowedFields,
                                           Set<String> deniedFields,
                                           Map<String, Set<String>> fieldOperators,
                                           int maxDepth,
                                           int maxNodes,
                                           int maxCollectionSize,
                                           int maxStringLength) {
        return new StructuredConditionPolicy(operators,
                                             allowedFields,
                                             allowedFieldsRestricted,
                                             deniedFields,
                                             fieldOperators,
                                             maxDepth,
                                             maxNodes,
                                             maxCollectionSize,
                                             maxStringLength,
                                             terms);
    }

    private static Map<String, Set<String>> copyFieldOperators(Map<String, Set<String>> fieldOperators) {
        Map<String, Set<String>> copied = new LinkedHashMap<>();
        for (Map.Entry<String, Set<String>> entry : fieldOperators.entrySet()) {
            copied.put(ConditionNames.normalize(entry.getKey(), "structured condition field operator field"),
                       Set.copyOf(entry.getValue()));
        }
        return Map.copyOf(copied);
    }

    private static Map<String, String> defaultOperators() {
        Map<String, String> operators = new LinkedHashMap<>();
        operators.put("eq", "=");
        operators.put("=", "=");
        operators.put("gt", ">");
        operators.put(">", ">");
        operators.put("lt", "<");
        operators.put("<", "<");
        operators.put("ge", ">=");
        operators.put(">=", ">=");
        operators.put("le", "<=");
        operators.put("<=", "<=");
        operators.put("ne", "<>");
        operators.put("!=", "!=");
        operators.put("<>", "<>");
        operators.put("like", "like");
        operators.put("not-like", "not-like");
        operators.put("like-ignore-case", "like-ignore-case");
        operators.put("not-like-ignore-case", "not-like-ignore-case");
        operators.put("in", "in");
        operators.put("not-in", "not-in");
        operators.put("between", "between");
        operators.put("not-between", "not-between");
        operators.put("is-null", "is-null");
        operators.put("is-not-null", "is-not-null");
        return operators;
    }

    private static int requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be greater than zero");
        }
        return value;
    }

    private static int requireAtMost(int value, int limit, String name) {
        int positive = requirePositive(value, name);
        if (positive > limit) {
            throw new IllegalArgumentException(name + " must not exceed " + limit);
        }
        return positive;
    }

    private static int requireDepth(int value) {
        int depth = requirePositive(value, "structured condition max depth");
        if (depth > MAX_DEPTH_LIMIT) {
            throw new IllegalArgumentException("structured condition max depth must not exceed " + MAX_DEPTH_LIMIT);
        }
        return depth;
    }
}
