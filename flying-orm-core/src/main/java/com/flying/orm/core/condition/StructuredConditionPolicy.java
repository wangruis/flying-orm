package com.flying.orm.core.condition;

import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.internal.Names;

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
    private final Set<FieldIdentity> allowedFields;
    /** false 表示调用方从未设置字段白名单；true 时空集合明确表示一个字段都不允许。 */
    private final boolean allowedFieldsRestricted;
    private final Set<FieldIdentity> deniedFields;
    private final Map<FieldIdentity, Set<String>> fieldOperators;
    private final int maxDepth;
    private final int maxNodes;
    private final int maxCollectionSize;
    private final int maxStringLength;
    private final TermRegistry terms;

    private StructuredConditionPolicy(Map<String, String> operators,
                                      Set<FieldIdentity> allowedFields,
                                      boolean allowedFieldsRestricted,
                                      Set<FieldIdentity> deniedFields,
                                      Map<FieldIdentity, Set<String>> fieldOperators,
                                      int maxDepth,
                                      int maxNodes,
                                      int maxCollectionSize,
                                      int maxStringLength,
                                      TermRegistry terms) {
        this.operators = Objects.requireNonNull(operators, "condition operators must not be null");
        this.allowedFields = Objects.requireNonNull(allowedFields, "allowed fields must not be null");
        this.allowedFieldsRestricted = allowedFieldsRestricted;
        this.deniedFields = Objects.requireNonNull(deniedFields, "denied fields must not be null");
        this.fieldOperators = Objects.requireNonNull(fieldOperators, "field operators must not be null");
        this.maxDepth = maxDepth;
        this.maxNodes = maxNodes;
        this.maxCollectionSize = maxCollectionSize;
        this.maxStringLength = maxStringLength;
        this.terms = Objects.requireNonNull(terms, "term registry must not be null");
    }

    /**
     * 默认只开放常用参数化条件，自定义条件需要调用方显式放行。
     *
     * @return 默认策略
     */
    public static StructuredConditionPolicy defaults() {
        return new StructuredConditionPolicy(TermRegistry.standardOperators(),
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
        copiedOperators.put(Names.key(externalOperator, "external condition operator"),
                            Names.key(internalOperator, "internal condition operator"));
        return copy(Map.copyOf(copiedOperators),
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
        Set<FieldIdentity> copiedFields = new LinkedHashSet<>();
        for (String field : fields) {
            copiedFields.add(FieldIdentity.of(field));
        }
        return new StructuredConditionPolicy(operators,
                                             Set.copyOf(copiedFields),
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
        Set<FieldIdentity> copiedFields = new LinkedHashSet<>(deniedFields);
        for (String field : fields) {
            copiedFields.add(FieldIdentity.of(field));
        }
        return copy(operators,
                    allowedFields,
                    Set.copyOf(copiedFields),
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
        FieldIdentity identity = FieldIdentity.of(field);
        Set<String> normalizedOperators = new LinkedHashSet<>();
        for (String operator : operators) {
            normalizedOperators.add(Names.key(operator, "structured condition field operator"));
        }
        if (normalizedOperators.isEmpty()) {
            throw new IllegalArgumentException("structured condition field operators must not be empty");
        }
        Map<FieldIdentity, Set<String>> copiedFieldOperators = new LinkedHashMap<>(fieldOperators);
        copiedFieldOperators.put(identity, Set.copyOf(normalizedOperators));
        return copy(this.operators,
                    allowedFields,
                    deniedFields,
                    Map.copyOf(copiedFieldOperators),
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
        return copy(operators, allowedFields, deniedFields, fieldOperators,
                    requireDepth(maxDepth), maxNodes, maxCollectionSize, maxStringLength);
    }

    /**
     * 设置最多允许多少个条件节点。
     *
     * @param maxNodes 最大节点数
     * @return 新策略
     */
    public StructuredConditionPolicy withMaxNodes(int maxNodes) {
        return copy(operators, allowedFields, deniedFields, fieldOperators,
                    maxDepth,
                    requireAtMost(maxNodes, MAX_NODES_LIMIT, "structured condition max nodes"),
                    maxCollectionSize,
                    maxStringLength);
    }

    /**
     * 设置 in 或自定义集合值的最大元素数。
     *
     * @param maxCollectionSize 最大集合大小
     * @return 新策略
     */
    public StructuredConditionPolicy withMaxCollectionSize(int maxCollectionSize) {
        return copy(operators, allowedFields, deniedFields, fieldOperators,
                    maxDepth,
                    maxNodes,
                    requireAtMost(maxCollectionSize,
                                  MAX_COLLECTION_SIZE_LIMIT,
                                  "structured condition max collection size"),
                    maxStringLength);
    }

    /**
     * 设置单个字符串值最大长度。
     *
     * @param maxStringLength 最大字符串长度
     * @return 新策略
     */
    public StructuredConditionPolicy withMaxStringLength(int maxStringLength) {
        return copy(operators, allowedFields, deniedFields, fieldOperators,
                    maxDepth,
                    maxNodes,
                    maxCollectionSize,
                    requirePositive(maxStringLength, "structured condition max string length"));
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
                                             Objects.requireNonNull(terms, "term registry must not be null"));
    }

    /**
     * 在保留当前扩展声明的前提下追加受治理的 term。
     *
     * <p>这个入口供可以组合的结构化条件定制器使用。同名声明必须具有完全相同的值形状和显式
     * 描述器；没有新增声明时返回当前策略，不产生额外策略对象。</p>
     *
     * @param additions 要追加的受治理 term
     * @return 合并后的策略；注册表没有变化时返回当前实例
     */
    public StructuredConditionPolicy withAdditionalTerms(TermRegistry additions) {
        TermRegistry merged = terms.mergeDescribed(additions);
        return merged == terms ? this : withTerms(merged);
    }

    Optional<String> resolveOperator(String externalOperator) {
        return Optional.ofNullable(operators.get(Names.key(externalOperator, "condition operator")));
    }

    boolean allowsField(String field) {
        return allowsField(FieldIdentity.of(field));
    }

    boolean allowsField(FieldIdentity identity) {
        return !deniedFields.contains(identity)
                && (!allowedFieldsRestricted || allowedFields.contains(identity));
    }

    boolean allowsFieldOperator(String field, String externalOperator) {
        return allowsFieldOperator(FieldIdentity.of(field), externalOperator);
    }

    boolean allowsFieldOperator(FieldIdentity identity, String externalOperator) {
        Set<String> allowedOperators = fieldOperators.get(identity);
        return allowedOperators == null
                || allowedOperators.contains(Names.key(externalOperator, "condition operator"));
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
        String operator = Names.key(internalOperator, "condition operator");
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
        String operator = Names.key(internalOperator, "condition operator");
        return TermRegistry.standard().find(operator).isPresent();
    }

    /**
     * 结构化外部条件把扩展声明的复杂度折算进既有 node budget，不再建立第二套请求预算。
     * 标准 term 仍固定只计一个节点；自定义 term 必须来自带 descriptor 的受控注册表。
     */
    int termComplexityCost(String internalOperator) {
        return terms.governedComplexityCost(internalOperator);
    }

    private StructuredConditionPolicy copy(Map<String, String> operators,
                                           Set<FieldIdentity> allowedFields,
                                           Set<FieldIdentity> deniedFields,
                                           Map<FieldIdentity, Set<String>> fieldOperators,
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
