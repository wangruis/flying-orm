package com.flying.orm.core.condition;

import com.flying.orm.core.internal.Names;
import com.flying.orm.core.internal.hash.StableDigest;
import com.flying.orm.core.internal.hash.StableEncoder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * term 注册表保存内置和业务自定义条件处理器，使通用条件扩展不依赖硬编码分支。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public final class TermRegistry {

    private static final StableDigest.Domain DESCRIPTOR_FINGERPRINT_DOMAIN =
            StableDigest.domain("term-registry-descriptors/v1");

    private static final DescriptorState NO_DESCRIPTORS = new DescriptorState(
            false,
            StableDigest.sha256(DESCRIPTOR_FINGERPRINT_DOMAIN)
                        .integer("DESCRIPTOR_COUNT", 0)
                        .finishHex());

    /** 外部常用别名到标准 term id 的固定映射，所有默认策略直接共享这份只读表。 */
    private static final Map<String, String> STANDARD_OPERATORS = createStandardOperators();

    private static final TermRegistry EMPTY = new TermRegistry(List.of());

    private static final TermRegistry STANDARD = new TermRegistry(List.of(
            TermHandler.simple("is-null", ConditionValueShape.NONE),
            TermHandler.simple("is-not-null", ConditionValueShape.NONE),
            TermHandler.simple("=", ConditionValueShape.SCALAR),
            TermHandler.simple("!=", ConditionValueShape.SCALAR),
            TermHandler.simple("<>", ConditionValueShape.SCALAR),
            TermHandler.simple(">", ConditionValueShape.SCALAR),
            TermHandler.simple(">=", ConditionValueShape.SCALAR),
            TermHandler.simple("<", ConditionValueShape.SCALAR),
            TermHandler.simple("<=", ConditionValueShape.SCALAR),
            TermHandler.simple("like", ConditionValueShape.SCALAR),
            TermHandler.simple("not-like", ConditionValueShape.SCALAR),
            TermHandler.simple("like-ignore-case", ConditionValueShape.SCALAR),
            TermHandler.simple("not-like-ignore-case", ConditionValueShape.SCALAR),
            TermHandler.simple("in", ConditionValueShape.COLLECTION),
            TermHandler.simple("not-in", ConditionValueShape.COLLECTION),
            TermHandler.simple("between", ConditionValueShape.RANGE),
            TermHandler.simple("not-between", ConditionValueShape.RANGE)));

    private final List<TermHandler> handlers;

    private final Map<String, TermHandler> handlersById;

    private final boolean hasDescriptors;

    private final String descriptorFingerprint;

    private TermRegistry(List<TermHandler> handlers) {
        List<TermHandler> copiedHandlers = List.copyOf(handlers);
        Map<String, TermHandler> indexedHandlers = new LinkedHashMap<>(Names.mapCapacity(copiedHandlers.size()));
        List<TermExtensionDescriptor> descriptors = new ArrayList<>();
        for (TermHandler handler : copiedHandlers) {
            TermHandler safeHandler = Objects.requireNonNull(handler, "term handler must not be null");
            String normalizedId = Names.key(safeHandler.id(), "term id");
            TermHandler previous = indexedHandlers.putIfAbsent(normalizedId, safeHandler);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate term id");
            }
            descriptor(safeHandler, normalizedId).ifPresent(descriptors::add);
        }
        this.handlers = copiedHandlers;
        this.handlersById = Map.copyOf(indexedHandlers);
        DescriptorState descriptorState = descriptorState(descriptors);
        this.hasDescriptors = descriptorState.present();
        this.descriptorFingerprint = descriptorState.fingerprint();
    }

    private TermRegistry(List<TermHandler> handlers,
                         Map<String, TermHandler> handlersById) {
        this.handlers = List.copyOf(Objects.requireNonNull(handlers, "term handlers must not be null"));
        this.handlersById = Map.copyOf(
                Objects.requireNonNull(handlersById, "indexed term handlers must not be null"));
        List<TermExtensionDescriptor> descriptors = new ArrayList<>();
        // Builder 已经按规范化 id 冻结了索引。这里直接复用索引键，避免发布时再次调用
        // 业务 handler.id()；自定义 handler 的 id/shape 在不可信边界各读取一次即可。
        for (Map.Entry<String, TermHandler> entry : this.handlersById.entrySet()) {
            descriptor(entry.getValue(), entry.getKey()).ifPresent(descriptors::add);
        }
        DescriptorState descriptorState = descriptorState(descriptors);
        this.hasDescriptors = descriptorState.present();
        this.descriptorFingerprint = descriptorState.fingerprint();
    }

    /**
     * 返回空 term 注册表。
     *
     * @return 空注册表
     */
    public static TermRegistry empty() {
        return EMPTY;
    }

    /**
     * 返回内置 term 的不可变值形状注册表。
     */
    public static TermRegistry standard() {
        return STANDARD;
    }

    /**
     * 创建 term 注册表构建器。
     *
     * @return term 注册表构建器
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * 返回只读 term 处理器集合。
     *
     * @return 只读 term 处理器集合
     */
    public List<TermHandler> handlers() {
        return handlers;
    }

    /**
     * 按规范化 term id 查找处理器。
     *
     * @param id term id
     * @return 匹配处理器；不存在时返回空
     */
    public Optional<TermHandler> find(String id) {
        return Optional.ofNullable(handlersById.get(Names.key(id, "term id")));
    }

    /**
     * 按规范化 term id 获取处理器，不存在时抛出确定性异常。
     *
     * @param id term id
     * @return 匹配处理器
     * @throws IllegalArgumentException term 不存在时抛出
     */
    public TermHandler handler(String id) {
        return find(id).orElseThrow(() -> new IllegalArgumentException("term does not exist"));
    }

    /** @return 当前注册表是否含显式治理描述器 */
    public boolean hasDescriptors() {
        return hasDescriptors;
    }

    /**
     * 返回只由显式描述器决定的冻结指纹。legacy handler 仍由注册表实例身份隔离，不伪造稳定身份。
     */
    public String descriptorFingerprint() {
        return descriptorFingerprint;
    }

    /** 按 term id 查找显式描述器；标准和 legacy trusted term 返回空。 */
    public Optional<TermExtensionDescriptor> descriptor(String id) {
        return find(id).flatMap(TermRegistry::safeDescriptor);
    }

    /** 返回默认结构化条件可以解析的标准 operator 映射。 */
    static Map<String, String> standardOperators() {
        return STANDARD_OPERATORS;
    }

    /**
     * 标准 term 固定消耗一个节点；受治理扩展使用描述器声明的复杂度。
     * 缺失或仍是 legacy handler 的扩展不能进入外部结构化查询。
     */
    int governedComplexityCost(String internalOperator) {
        String operator = Names.key(internalOperator, "condition operator");
        if (STANDARD.handlersById.containsKey(operator)) {
            return 1;
        }
        TermHandler handler = handlersById.get(operator);
        if (handler == null) {
            throw new IllegalArgumentException(
                    "governed term [" + operator + "] requires an explicit extension descriptor");
        }
        return safeDescriptor(handler).orElseThrow(() ->
                new IllegalArgumentException(
                        "governed term [" + operator + "] requires an explicit extension descriptor"))
                                      .complexityCost();
    }

    /**
     * 把一组带治理描述器的扩展 term 追加到当前注册表。
     *
     * <p>同名 term 只有在值形状和描述器完全一致时才视为同一份声明。旧式 handler 没有描述器，
     * 不能借由合并进入受治理的外部查询。没有新增声明时直接复用当前注册表。</p>
     */
    TermRegistry mergeDescribed(TermRegistry additions) {
        TermRegistry safeAdditions = Objects.requireNonNull(additions, "additional term registry must not be null");
        if (safeAdditions.handlersById.isEmpty()) {
            return this;
        }

        boolean changed = false;
        for (Map.Entry<String, TermHandler> entry : safeAdditions.handlersById.entrySet()) {
            String id = entry.getKey();
            TermHandler addition = entry.getValue();
            TermExtensionDescriptor additionDescriptor = safeDescriptor(addition).orElseThrow(() ->
                    new IllegalArgumentException(
                            "governed term [" + id + "] requires an explicit extension descriptor"));
            TermHandler existing = handlersById.get(id);
            if (existing == null) {
                changed = true;
                continue;
            }
            TermExtensionDescriptor existingDescriptor = safeDescriptor(existing).orElseThrow(() ->
                    new IllegalArgumentException(
                            "governed term [" + id + "] requires an explicit extension descriptor"));
            if (existing.shape() != addition.shape() || !existingDescriptor.equals(additionDescriptor)) {
                throw new IllegalArgumentException("conflicting governed term declaration: " + id);
            }
        }
        if (!changed) {
            return this;
        }

        List<TermHandler> mergedHandlers = new ArrayList<>(handlers.size() + safeAdditions.handlers.size());
        mergedHandlers.addAll(handlers);
        Map<String, TermHandler> mergedIndex = new LinkedHashMap<>(handlersById);
        for (Map.Entry<String, TermHandler> entry : safeAdditions.handlersById.entrySet()) {
            if (mergedIndex.putIfAbsent(entry.getKey(), entry.getValue()) == null) {
                mergedHandlers.add(entry.getValue());
            }
        }
        return new TermRegistry(mergedHandlers, mergedIndex);
    }

    private static Optional<TermExtensionDescriptor> descriptor(TermHandler handler, String normalizedId) {
        Optional<TermExtensionDescriptor> descriptor = safeDescriptor(handler);
        descriptor.ifPresent(value -> {
            if (!normalizedId.equals(value.id())) {
                throw new IllegalArgumentException("term extension descriptor id must match handler id");
            }
        });
        return descriptor;
    }

    private static Optional<TermExtensionDescriptor> safeDescriptor(TermHandler handler) {
        return Objects.requireNonNull(
                Objects.requireNonNull(handler, "term handler must not be null").descriptor(),
                "term extension descriptor lookup must not return null");
    }

    private static DescriptorState descriptorState(List<TermExtensionDescriptor> descriptors) {
        if (descriptors.isEmpty()) {
            return NO_DESCRIPTORS;
        }
        List<TermExtensionDescriptor> ordered = descriptors.stream()
                .sorted(java.util.Comparator.comparing(TermExtensionDescriptor::id)).toList();
        StableEncoder encoder = StableDigest.sha256(DESCRIPTOR_FINGERPRINT_DOMAIN)
                                            .integer("DESCRIPTOR_COUNT", ordered.size());
        for (TermExtensionDescriptor descriptor : ordered) {
            encoder.text("DESCRIPTOR", descriptor.fingerprint());
        }
        return new DescriptorState(!ordered.isEmpty(), encoder.finishHex());
    }

    private record DescriptorState(boolean present, String fingerprint) {
    }

    private static Map<String, String> createStandardOperators() {
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
        return Map.copyOf(operators);
    }

    /**
     * term 注册表构建器，用于发布前收集处理器。
     *
     * @author wangr
     * @date 2026-07-21
     * @version v1.0
     */
    public static final class Builder {

        private final List<TermHandler> handlers = new ArrayList<>();

        private boolean includesStandardHandlers;

        private Builder() {
        }

        /**
         * 添加 term 处理器。
         *
         * @param handler term 处理器
         * @return 当前构建器
         */
        public Builder add(TermHandler handler) {
            TermHandler safeHandler = Objects.requireNonNull(handler, "term handler must not be null");
            if (isStandardHandler(safeHandler)) {
                includesStandardHandlers = true;
            } else {
                handlers.add(safeHandler);
            }
            return this;
        }

        /**
         * 构建只读 term 注册表。
         *
         * @return term 注册表
         */
        public TermRegistry build() {
            if (handlers.isEmpty()) {
                return includesStandardHandlers ? STANDARD : EMPTY;
            }
            int combinedSize = handlers.size() + (includesStandardHandlers ? STANDARD.handlers.size() : 0);
            List<TermHandler> combined = new ArrayList<>(combinedSize);
            Map<String, TermHandler> indexed = new LinkedHashMap<>(Names.mapCapacity(combinedSize));
            if (includesStandardHandlers) {
                combined.addAll(STANDARD.handlers);
                indexed.putAll(STANDARD.handlersById);
            }
            for (TermHandler handler : handlers) {
                String id = Names.key(handler.id(), "term id");
                Objects.requireNonNull(handler.shape(), "term value shape must not be null");
                if (STANDARD.find(id).isPresent()) {
                    throw new IllegalArgumentException("standard term id must not be registered");
                }
                if (indexed.putIfAbsent(id, handler) != null) {
                    throw new IllegalArgumentException("duplicate term id");
                }
                combined.add(handler);
            }
            return new TermRegistry(combined, indexed);
        }

        private static boolean isStandardHandler(TermHandler handler) {
            for (TermHandler standard : STANDARD.handlers) {
                if (handler == standard) {
                    return true;
                }
            }
            return false;
        }
    }
}
