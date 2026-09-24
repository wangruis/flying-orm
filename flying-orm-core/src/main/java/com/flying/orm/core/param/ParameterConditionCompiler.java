package com.flying.orm.core.param;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.condition.TermHandler;
import com.flying.orm.core.condition.TermRegistry;
import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.internal.Names;
import com.flying.orm.core.internal.condition.ConditionValueNormalizer;
import com.flying.orm.core.internal.condition.ConditionValuePolicy;
import com.flying.orm.core.internal.value.BindableValueSnapshots;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 按预先声明的字段和 operator 规则，将请求参数 Map 编译为条件 AST。
 *
 * <p>负责名称归一化、空值、默认值、类型转换及 AND/OR 分组；只读输入 Map。
 * 字段和 operator 白名单校验及参数化 SQL 生成由后续阶段负责。</p>
 * <p>构造时复制规则并检查重复参数，之后无可变状态，可以并发复用。</p>
 *
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
public final class ParameterConditionCompiler {

    private static final int DEFAULT_MAX_COLLECTION_SIZE = 1_000;
    private static final int DEFAULT_MAX_STRING_LENGTH = 4_096;

    private final List<ParameterConditionSpec> specs;
    private final List<SpecGroup> groups;
    private final TermRegistry terms;
    private final Set<String> knownParameters;
    private final int maxCollectionSize;
    private final int maxStringLength;

    private ParameterConditionCompiler(List<PendingSpecGroup> groups,
                                       TermRegistry terms,
                                       int maxCollectionSize,
                                       int maxStringLength) {
        int safeMaxCollectionSize = requireCollectionSize(maxCollectionSize);
        int safeMaxStringLength = requirePositive(maxStringLength, "parameter condition max string length");
        this.terms = Objects.requireNonNull(terms, "term registry must not be null");
        List<SpecGroup> copiedGroups = groups.stream()
                                             .map(group -> group.compileDefaults(
                                                     this.terms,
                                                     safeMaxCollectionSize, safeMaxStringLength))
                                             .toList();
        validateDuplicateParameters(copiedGroups);
        this.groups = copiedGroups;
        LinkedHashSet<String> known = new LinkedHashSet<>();
        copiedGroups.forEach(group -> known.addAll(group.normalizedParameters()));
        this.knownParameters = Set.copyOf(known);
        this.maxCollectionSize = safeMaxCollectionSize;
        this.maxStringLength = safeMaxStringLength;
        this.specs = copiedGroups.stream()
                                 .flatMap(group -> group.specs().stream())
                                 .map(CompiledSpec::spec)
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
            group.compile(indexedParameters, builder, maxCollectionSize, maxStringLength);
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
        if (safeParameters.isEmpty() || knownParameters.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> indexedParameters = new LinkedHashMap<>(
                Names.mapCapacity(Math.min(safeParameters.size(), knownParameters.size())));
        for (Map.Entry<String, ?> entry : parameters.entrySet()) {
            String key = entry.getKey();
            if (key == null || key.isBlank()) {
                continue;
            }
            String normalizedKey = Names.key(key, "parameter name");
            if (!knownParameters.contains(normalizedKey)) {
                continue;
            }
            if (indexedParameters.containsKey(normalizedKey)) {
                throw new IllegalArgumentException("duplicate input parameter name");
            }
            indexedParameters.put(normalizedKey, entry.getValue());
        }
        return indexedParameters;
    }

    private static Object resolveValue(CompiledSpec compiledSpec,
                                       Map<String, ?> indexedParameters,
                                       int maxCollectionSize,
                                       int maxStringLength) {
        // 先把 Iterable 快照并清理一次，后面的 converter 和 AST 构建复用同一份值，单次迭代器不会被判空耗尽。
        ParameterConditionSpec spec = compiledSpec.spec();
        String parameter = spec.normalizedParameter();
        if (!indexedParameters.containsKey(parameter) && !spec.hasDefaultValue()) {
            return EmptyValue.INSTANCE;
        }
        Object value = indexedParameters.get(parameter);
        ConditionValueShape shape = compiledSpec.shape();
        Object normalized = normalizePresentValue(value, shape, maxCollectionSize, maxStringLength);
        boolean usesCompiledDefault = false;
        if (normalized == EmptyValue.INSTANCE && spec.hasDefaultValue()) {
            normalized = compiledSpec.defaultPresent()
                    ? spec.ownedDefaultValue() : EmptyValue.INSTANCE;
            usesCompiledDefault = compiledSpec.defaultPresent();
        }
        if (normalized == EmptyValue.INSTANCE) {
            return EmptyValue.INSTANCE;
        }
        Object conversionInput = usesCompiledDefault
                ? spec.isolateOwnedDefaultForConverter(normalized) : normalized;
        Object converted = spec.convert(conversionInput);
        return spec.usesIdentityConverter() ? converted
                : normalizePresentValue(converted, shape, maxCollectionSize, maxStringLength);
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

    private static CompiledSpec compileDefault(ParameterConditionSpec spec,
                                               TermRegistry terms,
                                               int maxCollectionSize,
                                               int maxStringLength) {
        ParameterConditionSpec safeSpec = Objects.requireNonNull(spec, "parameter condition spec must not be null");
        ConditionValueShape shape = valueShape(safeSpec.operator(), terms);
        boolean custom = TermRegistry.standard().find(safeSpec.operator()).isEmpty();
        if (!safeSpec.hasDefaultValue()) {
            return new CompiledSpec(safeSpec, false, shape, custom);
        }
        Object normalized = normalizePresentValue(safeSpec.ownedDefaultValue(),
                                                  shape,
                                                  maxCollectionSize,
                                                  maxStringLength);
        boolean present = normalized != EmptyValue.INSTANCE;
        Object defaultValue = present ? normalized : null;
        return new CompiledSpec(
                ParameterConditionSpec.compiled(safeSpec, defaultValue), present, shape, custom);
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

    private record PendingSpecGroup(boolean orGroup, List<ParameterConditionSpec> specs) {

        private PendingSpecGroup {
            specs = List.copyOf(Objects.requireNonNull(specs, "parameter condition specs must not be null"));
            if (specs.isEmpty()) {
                throw new IllegalArgumentException("parameter condition specs must not be empty");
            }
        }

        private SpecGroup compileDefaults(TermRegistry terms, int maxCollectionSize, int maxStringLength) {
            return new SpecGroup(orGroup, specs.stream()
                                               .map(spec -> compileDefault(
                                                       spec, terms, maxCollectionSize, maxStringLength))
                                               .toList());
        }
    }

    private record SpecGroup(boolean orGroup,
                             List<CompiledSpec> specs,
                             Set<String> repeatedIterableParameters) {

        private SpecGroup(boolean orGroup, List<CompiledSpec> specs) {
            this(orGroup, specs, repeatedIterableParameters(specs));
        }

        private SpecGroup {
            specs = List.copyOf(Objects.requireNonNull(specs, "compiled parameter specs must not be null"));
            repeatedIterableParameters = Set.copyOf(Objects.requireNonNull(
                    repeatedIterableParameters, "repeated iterable parameters must not be null"));
        }

        private Set<String> normalizedParameters() {
            Set<String> parameters = new LinkedHashSet<>();
            for (CompiledSpec compiledSpec : specs) {
                ParameterConditionSpec safeSpec = compiledSpec.spec();
                parameters.add(safeSpec.normalizedParameter());
            }
            return parameters;
        }

        private void compile(Map<String, ?> indexedParameters,
                             ConditionGroup.Builder builder,
                             int maxCollectionSize,
                             int maxStringLength) {
            if (orGroup) {
                List<CompiledTerm> compiledTerms = compileTerms(indexedParameters,
                                                                maxCollectionSize,
                                                                maxStringLength);
                if (!compiledTerms.isEmpty()) {
                    // 整组无有效值时不创建空括号；有值时保留一个明确的 OR 子组。
                    builder.or(or -> compiledTerms.forEach(term -> publish(or, term)));
                }
                return;
            }
            CompiledSpec compiledSpec = specs.get(0);
            ParameterConditionSpec spec = compiledSpec.spec();
            Object value = resolveValue(compiledSpec, indexedParameters, maxCollectionSize, maxStringLength);
            if (value != EmptyValue.INSTANCE) {
                publish(builder, new CompiledTerm(
                        spec.identity(), spec.operator(), value,
                        compiledSpec.shape(), compiledSpec.custom()));
            }
        }

        private static void publish(ConditionGroup.Builder builder,
                                    CompiledTerm term) {
            boolean customCollection = term.custom()
                    && term.shape() != ConditionValueShape.NONE
                    && term.shape() != ConditionValueShape.SCALAR
                    && term.value() instanceof List<?>;
            Object value = term.custom()
                    ? snapshotCustomValue(term.value(), customCollection)
                    : term.value();
            builder.add(TermCondition.of(term.identity(), term.operator(), value));
        }

        /** 自定义 term 仍由注册项解释；这里只完成公开 AST 所需的值所有权交接。 */
        private static Object snapshotCustomValue(Object value, boolean collection) {
            if (!collection) {
                return BindableValueSnapshots.logicalScalar(value);
            }
            return BindableValueSnapshots.logicalValues((List<?>) value);
        }

        private List<CompiledTerm> compileTerms(Map<String, ?> indexedParameters,
                                                int maxCollectionSize,
                                                int maxStringLength) {
            Map<String, ?> stableParameters = stableRepeatedIterables(
                    indexedParameters, maxCollectionSize);
            List<CompiledTerm> compiledTerms = new ArrayList<>(specs.size());
            for (CompiledSpec compiledSpec : specs) {
                ParameterConditionSpec spec = compiledSpec.spec();
                Object value = resolveValue(
                        compiledSpec, stableParameters, maxCollectionSize, maxStringLength);
                if (value != EmptyValue.INSTANCE) {
                    compiledTerms.add(new CompiledTerm(
                            spec.identity(), spec.operator(), value,
                            compiledSpec.shape(), compiledSpec.custom()));
                }
            }
            return compiledTerms;
        }

        /** 同组复用的请求 Iterable 先稳定化一次；每条规则仍独立执行形状、默认值和 converter。 */
        private Map<String, ?> stableRepeatedIterables(Map<String, ?> indexedParameters,
                                                       int maxCollectionSize) {
            Map<String, Object> stable = null;
            for (String parameter : repeatedIterableParameters) {
                if (!indexedParameters.containsKey(parameter)) {
                    continue;
                }
                Object value = indexedParameters.get(parameter);
                if (!(value instanceof Iterable<?> iterable)
                        || value instanceof java.util.Collection<?>) {
                    continue;
                }
                if (stable == null) {
                    stable = new LinkedHashMap<>(indexedParameters);
                }
                stable.put(parameter, snapshotIterable(iterable, maxCollectionSize));
            }
            return stable == null ? indexedParameters : stable;
        }

        private static Set<String> repeatedIterableParameters(List<CompiledSpec> specs) {
            Set<String> seen = new LinkedHashSet<>();
            Set<String> repeated = new LinkedHashSet<>();
            Set<String> iterable = new LinkedHashSet<>();
            for (CompiledSpec compiledSpec : specs) {
                ParameterConditionSpec spec = compiledSpec.spec();
                String parameter = spec.normalizedParameter();
                ConditionValueShape shape = compiledSpec.shape();
                boolean acceptsIterable = shape == ConditionValueShape.COLLECTION
                        || shape == ConditionValueShape.RANGE
                        || shape == ConditionValueShape.SCALAR_OR_COLLECTION;
                if (!seen.add(parameter)) {
                    repeated.add(parameter);
                }
                if (acceptsIterable) {
                    iterable.add(parameter);
                }
            }
            repeated.retainAll(iterable);
            return repeated;
        }

        private static List<Object> snapshotIterable(Iterable<?> source, int maxCollectionSize) {
            List<Object> values = new ArrayList<>(Math.min(maxCollectionSize, 16));
            for (Object value : source) {
                values.add(value);
                if (values.size() > maxCollectionSize) {
                    break;
                }
            }
            return Collections.unmodifiableList(values);
        }
    }

    private record CompiledTerm(FieldIdentity identity,
                                String operator,
                                Object value,
                                ConditionValueShape shape,
                                boolean custom) {
    }

    private record CompiledSpec(ParameterConditionSpec spec,
                                boolean defaultPresent,
                                ConditionValueShape shape,
                                boolean custom) {

        private CompiledSpec {
            spec = Objects.requireNonNull(spec, "parameter condition spec must not be null");
            shape = Objects.requireNonNull(shape, "parameter condition value shape must not be null");
        }
    }

    /**
     * ParameterConditionCompiler 构建器。
     *
     * @author wangr
     * @date 2026-07-22
     * @version v1.0
     */
    public static final class Builder {

        private final List<PendingSpecGroup> groups = new ArrayList<>();
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
            groups.add(new PendingSpecGroup(false, List.of(Objects.requireNonNull(
                    spec, "parameter condition spec must not be null"))));
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
            groups.add(new PendingSpecGroup(true, List.of(Objects.requireNonNull(
                    specs, "parameter condition specs must not be null"))));
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
        return requirePositive(value, "parameter condition max collection size");
    }
}
