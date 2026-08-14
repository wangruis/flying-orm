package com.flying.orm.core.param;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.internal.condition.ConditionValueNormalizer;
import com.flying.orm.core.internal.condition.ConditionValuePolicy;
import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.condition.TermHandler;
import com.flying.orm.core.condition.TermRegistry;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 把请求参数 Map 按预先声明的 Java 规则编译成条件 AST，是“参数驱动动态条件”的轻量入口。
 *
 * <p>调用方只声明“哪个参数对应哪个字段和 operator”，这里负责名称归一化、空值策略、默认值、类型转换
 * 以及 AND/OR 分组。输出仍是结构化条件，不生成 SQL；字段白名单、operator 白名单和最终参数化渲染继续由
 * 后面的条件编译与 SQL 渲染阶段负责。</p>
 *
 * <p>编译器在构造时复制规则并检查重复参数，之后没有可变状态，可以并发复用。输入 Map 也只读取不修改。</p>
 *
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
public final class ParameterConditionCompiler {

    private static final int DEFAULT_MAX_COLLECTION_SIZE = 1_000;

    private static final int MAX_COLLECTION_SIZE_LIMIT = 1_000;

    private static final int DEFAULT_MAX_STRING_LENGTH = 4_096;

    private final List<ParameterConditionSpec> specs;

    private final List<SpecGroup> groups;

    private final TermRegistry terms;

    private final int maxCollectionSize;

    private final int maxStringLength;

    private ParameterConditionCompiler(List<SpecGroup> groups,
                                       TermRegistry terms,
                                       int maxCollectionSize,
                                       int maxStringLength) {
        int safeMaxCollectionSize = requireCollectionSize(maxCollectionSize);
        int safeMaxStringLength = requirePositive(maxStringLength, "parameter condition max string length");
        this.terms = Objects.requireNonNull(terms, "term registry must not be null");
        // 规则冲突在启动/装配阶段一次发现，不能等某个请求碰巧带上该参数才暴露。
        List<SpecGroup> copiedGroups = groups.stream()
                                             .map(group -> group.snapshotDefaults(
                                                     this.terms,
                                                     safeMaxCollectionSize, safeMaxStringLength))
                                             .toList();
        validateDuplicateParameters(copiedGroups);
        this.groups = copiedGroups;
        this.maxCollectionSize = safeMaxCollectionSize;
        this.maxStringLength = safeMaxStringLength;
        this.specs = copiedGroups.stream()
                                 .flatMap(group -> group.specs().stream())
                                 .toList();
    }

    /**
     * 创建参数条件编译器构建器。
     *
     * @return 参数条件编译器构建器
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * 返回只读规则列表。
     *
     * @return 只读规则列表
     */
    public List<ParameterConditionSpec> specs() {
        return specs;
    }

    /**
     * 将参数 Map 编译为顶层 AND 条件组。声明为 OR 组的规则会作为一个整体嵌入，括号语义不会丢失。
     *
     * @param parameters 请求参数
     * @return AND 条件组
     */
    public ConditionGroup compile(Map<String, ?> parameters) {
        Map<String, ?> indexedParameters = indexParameters(parameters);
        ConditionGroup.Builder builder = ConditionGroup.and(terms);
        for (SpecGroup group : groups) {
            group.compile(indexedParameters, builder, terms, maxCollectionSize, maxStringLength);
        }
        return builder.build();
    }

    private void validateDuplicateParameters(List<SpecGroup> groups) {
        // 参数名按忽略大小写的规范名判重，避免 userId 和 USERID 同时命中两条规则。
        Set<String> seen = new LinkedHashSet<>();
        for (SpecGroup group : groups) {
            Set<String> groupParameters = group.normalizedParameters();
            for (String parameter : groupParameters) {
                if (!seen.add(parameter)) {
                    throw new IllegalArgumentException("duplicate parameter condition spec");
                }
            }
        }
    }

    private Map<String, ?> indexParameters(Map<String, ?> parameters) {
        // 先建一次规范名索引，后面每条规则都是 O(1) 查找，也统一处理大小写差异。
        Map<String, ?> safeParameters = Objects.requireNonNull(parameters, "parameters must not be null");
        Map<String, Object> indexedParameters = new LinkedHashMap<>(ParameterNames.mapCapacity(safeParameters.size()));
        for (Map.Entry<String, ?> entry : parameters.entrySet()) {
            String normalizedKey = ParameterNames.normalize(entry.getKey(), "parameter name");
            if (indexedParameters.containsKey(normalizedKey)) {
                throw new IllegalArgumentException("duplicate input parameter name");
            }
            indexedParameters.put(normalizedKey, entry.getValue());
        }
        return indexedParameters;
    }

    private static Object resolveValue(ParameterConditionSpec spec,
                                       Map<String, ?> indexedParameters,
                                       TermRegistry terms,
                                       int maxCollectionSize,
                                       int maxStringLength) {
        // 先把 Iterable 快照并清理一次，后面的 converter 和 AST 构建复用同一份值，单次迭代器不会被判空耗尽。
        String parameter = spec.normalizedParameter();
        Object value = indexedParameters.containsKey(parameter) ? indexedParameters.get(parameter) : null;
        ConditionValueShape shape = valueShape(spec.operator(), terms);
        Object normalized = normalizePresentValue(value, shape, maxCollectionSize, maxStringLength);
        if (normalized == EmptyValue.INSTANCE && spec.hasDefaultValue()) {
            normalized = normalizePresentValue(spec.defaultValue(), shape, maxCollectionSize, maxStringLength);
        }
        if (normalized == EmptyValue.INSTANCE) {
            return EmptyValue.INSTANCE;
        }
        return normalizePresentValue(spec.convert(normalized), shape, maxCollectionSize, maxStringLength);
    }

    private static Object normalizePresentValue(Object value,
                                                 ConditionValueShape shape,
                                                 int maxCollectionSize,
                                                 int maxStringLength) {
        ConditionValueNormalizer.Result result = ConditionValueNormalizer.normalize(
                shape,
                value,
                ConditionValuePolicy.IGNORE_EMPTY,
                (scalar, index) -> scalar,
                maxCollectionSize,
                maxStringLength);
        return result.present() ? result.value() : EmptyValue.INSTANCE;
    }

    private static ParameterConditionSpec snapshotDefault(ParameterConditionSpec spec,
                                                          TermRegistry terms,
                                                          int maxCollectionSize,
                                                          int maxStringLength) {
        ParameterConditionSpec safeSpec = Objects.requireNonNull(spec, "parameter condition spec must not be null");
        if (!safeSpec.hasDefaultValue()) {
            return safeSpec;
        }
        Object normalized = normalizePresentValue(safeSpec.defaultValue(),
                                                  valueShape(safeSpec.operator(), terms),
                                                  maxCollectionSize,
                                                  maxStringLength);
        Object snapshot = normalized == EmptyValue.INSTANCE ? null : snapshotArray(normalized);
        return new ParameterConditionSpec(safeSpec.parameter(), safeSpec.field(), safeSpec.operator(), snapshot,
                                          true, safeSpec.converter());
    }

    /**
     * 标量数组不会被值归一化器拆成集合；默认值仍必须在构建期隔离调用方的可变数组。
     * 集合和范围值已经由归一化器生成不可变列表，无需再次复制。
     */
    private static Object snapshotArray(Object value) {
        if (value == null || !value.getClass().isArray()) {
            return value;
        }
        int length = Array.getLength(value);
        Object copy = Array.newInstance(value.getClass().getComponentType(), length);
        System.arraycopy(value, 0, copy, 0, length);
        return copy;
    }

    private static ConditionValueShape valueShape(String operator, TermRegistry terms) {
        return TermRegistry.standard()
                           .find(operator)
                           .or(() -> terms.find(operator))
                           .map(TermHandler::shape)
                           .orElse(ConditionValueShape.SCALAR);
    }

    private static int requirePositive(int value, String name) {
        if (value < 1) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    /** 用专用哨兵区分“条件应跳过”和业务值 null，避免返回值语义含糊。 */
    private enum EmptyValue {
        INSTANCE
    }

    private record SpecGroup(boolean orGroup, List<ParameterConditionSpec> specs) {

        private SpecGroup {
            specs = List.copyOf(Objects.requireNonNull(specs, "parameter condition specs must not be null"));
            if (specs.isEmpty()) {
                throw new IllegalArgumentException("parameter condition specs must not be empty");
            }
        }

        private Set<String> normalizedParameters() {
            Set<String> parameters = new LinkedHashSet<>();
            for (ParameterConditionSpec spec : specs) {
                ParameterConditionSpec safeSpec = Objects.requireNonNull(spec,
                                                                         "parameter condition spec must not be null");
                parameters.add(safeSpec.normalizedParameter());
            }
            return parameters;
        }

        private SpecGroup snapshotDefaults(TermRegistry terms, int maxCollectionSize, int maxStringLength) {
            return new SpecGroup(orGroup, specs.stream()
                                                .map(spec -> snapshotDefault(
                                                        spec, terms, maxCollectionSize, maxStringLength))
                                                .toList());
        }

        private void compile(Map<String, ?> indexedParameters,
                             ConditionGroup.Builder builder,
                             TermRegistry terms,
                             int maxCollectionSize,
                             int maxStringLength) {
            if (orGroup) {
                List<CompiledTerm> compiledTerms = compileTerms(indexedParameters,
                                                                terms,
                                                                maxCollectionSize,
                                                                maxStringLength);
                if (!compiledTerms.isEmpty()) {
                    // 整组无有效值时不创建空括号；有值时保留一个明确的 OR 子组。
                    builder.or(or -> compiledTerms.forEach(term -> or.whereIfPresent(term.field(),
                                                                                    term.operator(),
                                                                                    term.value())));
                }
                return;
            }
            ParameterConditionSpec spec = specs.get(0);
            Object value = resolveValue(spec, indexedParameters, terms, maxCollectionSize, maxStringLength);
            if (value != EmptyValue.INSTANCE) {
                builder.whereIfPresent(spec.field(), spec.operator(), value);
            }
        }

        private List<CompiledTerm> compileTerms(Map<String, ?> indexedParameters,
                                                TermRegistry terms,
                                                int maxCollectionSize,
                                                int maxStringLength) {
            List<CompiledTerm> compiledTerms = new ArrayList<>(specs.size());
            for (ParameterConditionSpec spec : specs) {
                Object value = resolveValue(spec, indexedParameters, terms, maxCollectionSize, maxStringLength);
                if (value != EmptyValue.INSTANCE) {
                    compiledTerms.add(new CompiledTerm(spec.field(), spec.operator(), value));
                }
            }
            return compiledTerms;
        }
    }

    private record CompiledTerm(String field, String operator, Object value) {
    }

    /**
     * ParameterConditionCompiler 构建器。
     *
     * @author wangr
     * @date 2026-07-22
     * @version v1.0
     */
    public static final class Builder {

        private final List<SpecGroup> groups = new ArrayList<>();

        private final List<TermHandler> terms = new ArrayList<>();

        private int maxCollectionSize = DEFAULT_MAX_COLLECTION_SIZE;

        private int maxStringLength = DEFAULT_MAX_STRING_LENGTH;

        private Builder() {
        }

        /**
         * 添加参数条件规则。
         *
         * @param spec 参数条件规则
         * @return 当前构建器
         */
        public Builder add(ParameterConditionSpec spec) {
            groups.add(new SpecGroup(false, List.of(Objects.requireNonNull(spec,
                                                                           "parameter condition spec must not be null"))));
            return this;
        }

        /**
         * 给业务 term 追加明确值形状。可以多次调用，也可以和 {@link #addPackage(ParameterConditionPackage)}
         * 混用；构建顺序不会清掉已经收集的 term。重复 id 会在 build 时明确失败。
         */
        public Builder terms(TermRegistry terms) {
            this.terms.addAll(Objects.requireNonNull(terms, "term registry must not be null").handlers());
            return this;
        }

        /**
         * 添加参数条件命名包，适合一次注册某个业务领域的多个请求参数映射规则。
         *
         * @param conditionPackage 参数条件命名包
         * @return 当前构建器
         */
        public Builder addPackage(ParameterConditionPackage conditionPackage) {
            ParameterConditionPackage safePackage = Objects.requireNonNull(
                    conditionPackage, "parameter condition package must not be null");
            safePackage.specs().forEach(this::add);
            terms.addAll(safePackage.terms().handlers());
            return this;
        }

        /**
         * 添加 OR 参数条件组。
         *
         * @param specs 参数条件规则集合
         * @return 当前构建器
         */
        public Builder addOrGroup(ParameterConditionSpec... specs) {
            groups.add(new SpecGroup(true, List.of(Objects.requireNonNull(specs,
                                                                          "parameter condition specs must not be null"))));
            return this;
        }

        /**
         * 限制单个参数条件允许携带的集合项数。超过上限时会立刻停止读取 Iterable。
         *
         * @param maxCollectionSize 最大集合项数
         * @return 当前构建器
         */
        public Builder maxCollectionSize(int maxCollectionSize) {
            this.maxCollectionSize = requireCollectionSize(maxCollectionSize);
            return this;
        }

        /**
         * 限制参数条件中单个字符串去掉首尾空白后的最大长度。
         *
         * @param maxStringLength 最大字符串长度
         * @return 当前构建器
         */
        public Builder maxStringLength(int maxStringLength) {
            this.maxStringLength = requirePositive(maxStringLength,
                                                   "parameter condition max string length");
            return this;
        }

        /**
         * 构建参数条件编译器。
         *
         * @return 参数条件编译器
         */
        public ParameterConditionCompiler build() {
            TermRegistry.Builder termRegistry = TermRegistry.builder();
            terms.forEach(termRegistry::add);
            return new ParameterConditionCompiler(groups,
                                                  termRegistry.build(),
                                                  maxCollectionSize,
                                                  maxStringLength);
        }
    }

    private static int requireCollectionSize(int value) {
        int positive = requirePositive(value, "parameter condition max collection size");
        if (positive > MAX_COLLECTION_SIZE_LIMIT) {
            throw new IllegalArgumentException(
                    "parameter condition max collection size must not exceed " + MAX_COLLECTION_SIZE_LIMIT);
        }
        return positive;
    }
}
