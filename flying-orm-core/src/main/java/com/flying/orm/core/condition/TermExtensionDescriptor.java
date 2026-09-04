package com.flying.orm.core.condition;

import com.flying.orm.core.internal.Names;
import com.flying.orm.core.internal.hash.StableDigest;
import com.flying.orm.core.internal.hash.StableEncoder;
import com.flying.orm.core.scope.FieldUse;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 结构化扩展条件在进入可配置查询前必须公开的最小事实。
 *
 * <p>描述器只说明稳定身份、字段用途、需要的数据库能力，以及一次渲染承诺的参数和复杂度上限；
 * 它不持有 renderer、连接或请求值。旧 handler 没有描述器时仍可作为启动期可信扩展使用，但受治理的
 * 外部查询会明确拒绝它，避免把任意业务 renderer 暴露成请求可选插件。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class TermExtensionDescriptor {

    private static final StableDigest.Domain FINGERPRINT_DOMAIN =
            StableDigest.domain("term-extension-descriptor/v1");

    private final String id;
    private final FieldUse fieldUse;
    private final Set<String> requiredCapabilities;
    private final int maxParameters;
    private final int complexityCost;
    private final String fingerprint;

    private TermExtensionDescriptor(String id,
                                    FieldUse fieldUse,
                                    Collection<String> requiredCapabilities,
                                    int maxParameters,
                                    int complexityCost) {
        this.id = Names.key(id, "term extension id");
        this.fieldUse = Objects.requireNonNull(fieldUse, "term extension field use must not be null");
        if (fieldUse != FieldUse.FILTER) {
            throw new IllegalArgumentException("term extension field use must be FILTER");
        }
        this.requiredCapabilities = normalizeCapabilities(requiredCapabilities);
        if (maxParameters < 0) {
            throw new IllegalArgumentException("term extension max parameters must not be negative");
        }
        if (complexityCost < 1) {
            throw new IllegalArgumentException("term extension complexity cost must be positive");
        }
        this.maxParameters = maxParameters;
        this.complexityCost = complexityCost;
        this.fingerprint = computeFingerprint();
    }

    /** 创建只能用于 FILTER 的扩展描述器。 */
    public static TermExtensionDescriptor filter(String id,
                                                 Collection<String> requiredCapabilities,
                                                 int maxParameters,
                                                 int complexityCost) {
        return new TermExtensionDescriptor(
                id, FieldUse.FILTER, requiredCapabilities, maxParameters, complexityCost);
    }

    public String id() {
        return id;
    }

    public FieldUse fieldUse() {
        return fieldUse;
    }

    public Set<String> requiredCapabilities() {
        return requiredCapabilities;
    }

    public int maxParameters() {
        return maxParameters;
    }

    public int complexityCost() {
        return complexityCost;
    }

    /** 返回构造时一次生成的稳定摘要，供注册表和缓存身份复用。 */
    public String fingerprint() {
        return fingerprint;
    }

    /**
     * 在 SQL 和连接之前核对一次可配置扩展的能力与预算。
     *
     * @param availableCapabilities 当前数据库已确认的稳定能力 ID
     * @param parameterCount renderer 为当前 term 实际生成的参数数
     * @param availableComplexity 当前调用还允许消耗的复杂度单位
     */
    public void requireUsable(Set<String> availableCapabilities,
                              int parameterCount,
                              int availableComplexity) {
        Set<String> available = Objects.requireNonNull(
                availableCapabilities, "available dialect capabilities must not be null");
        for (String required : requiredCapabilities) {
            if (!containsCapability(available, required)) {
                throw new UnsupportedOperationException(
                        "term extension [" + id + "] requires dialect capability [" + required + "]");
            }
        }
        requireParameterCount(parameterCount);
        requireComplexityBudget(availableComplexity);
    }

    /** 核对 renderer 实际发布的参数数量没有超过装配时承诺的上限。 */
    public void requireParameterCount(int parameterCount) {
        if (parameterCount < 0 || parameterCount > maxParameters) {
            throw new IllegalArgumentException(
                    "term extension [" + id + "] parameter count exceeds declared maximum " + maxParameters);
        }
    }

    /** 核对调用方为当前扩展保留的复杂度预算。 */
    public void requireComplexityBudget(int availableComplexity) {
        if (availableComplexity < complexityCost) {
            throw new IllegalArgumentException(
                    "term extension [" + id + "] exceeds the available complexity budget");
        }
    }

    private String computeFingerprint() {
        List<String> orderedCapabilities = requiredCapabilities.stream().sorted().toList();
        StableEncoder encoder = StableDigest.sha256(FINGERPRINT_DOMAIN)
                                            .text("ID", id)
                                            .text("FIELD_USE", fieldUse.name())
                                            .integer("MAX_PARAMETERS", maxParameters)
                                            .integer("COMPLEXITY_COST", complexityCost)
                                            .integer("CAPABILITY_COUNT", orderedCapabilities.size());
        for (String capability : orderedCapabilities) {
            encoder.text("CAPABILITY", capability);
        }
        return encoder.finishHex();
    }

    private static Set<String> normalizeCapabilities(Collection<String> capabilities) {
        Collection<String> source = Objects.requireNonNull(
                capabilities, "term extension capabilities must not be null");
        if (source.isEmpty()) {
            return Set.of();
        }
        java.util.LinkedHashSet<String> normalized = new java.util.LinkedHashSet<>();
        for (String capability : source) {
            normalized.add(Names.key(capability, "term extension capability"));
        }
        return Set.copyOf(normalized);
    }

    private static boolean containsCapability(Set<String> available, String required) {
        if (available.contains(required)) {
            return true;
        }
        for (String candidate : available) {
            if (required.equals(Names.key(candidate, "available dialect capability"))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof TermExtensionDescriptor that
                && maxParameters == that.maxParameters
                && complexityCost == that.complexityCost
                && id.equals(that.id)
                && fieldUse == that.fieldUse
                && requiredCapabilities.equals(that.requiredCapabilities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, fieldUse, requiredCapabilities, maxParameters, complexityCost);
    }

    @Override
    public String toString() {
        return "TermExtensionDescriptor[id=" + id + ", capabilities=" + requiredCapabilities
                + ", maxParameters=" + maxParameters + ", complexityCost=" + complexityCost + ']';
    }
}
