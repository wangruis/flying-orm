package com.flying.orm.core.codec;

import com.flying.orm.core.internal.Names;
import com.flying.orm.core.internal.hash.StableDigest;
import com.flying.orm.core.internal.hash.StableEncoder;
import com.flying.orm.core.type.LogicalType;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 可配置值转换器的稳定适用范围。
 *
 * <p>Java 类型、逻辑数据库类型和方言能力在注册时冻结。描述器不参与逐值转换；注册表只在构造时读取
 * 一次并缓存指纹，因此未启用扩展时不会给普通读写热路径增加查表或分配。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class ValueCodecDescriptor {

    private static final StableDigest.Domain FINGERPRINT_DOMAIN =
            StableDigest.domain("value-codec-descriptor/v1");

    private final String id;
    private final Set<Class<?>> javaTypes;
    private final Set<LogicalType> logicalTypes;
    private final Set<String> requiredCapabilities;
    private final String fingerprint;

    private ValueCodecDescriptor(String id,
                                 Collection<Class<?>> javaTypes,
                                 Collection<LogicalType> logicalTypes,
                                 Collection<String> requiredCapabilities) {
        this.id = Names.key(id, "value codec id");
        this.javaTypes = copyTypes(javaTypes);
        this.logicalTypes = copyLogicalTypes(logicalTypes);
        this.requiredCapabilities = copyCapabilities(requiredCapabilities);
        if (this.javaTypes.isEmpty()) {
            throw new IllegalArgumentException("value codec Java types must not be empty");
        }
        if (this.logicalTypes.isEmpty()) {
            throw new IllegalArgumentException("value codec logical types must not be empty");
        }
        this.fingerprint = computeFingerprint();
    }

    public static ValueCodecDescriptor of(String id,
                                          Collection<Class<?>> javaTypes,
                                          Collection<LogicalType> logicalTypes,
                                          Collection<String> requiredCapabilities) {
        return new ValueCodecDescriptor(id, javaTypes, logicalTypes, requiredCapabilities);
    }

    public String id() {
        return id;
    }

    public Set<Class<?>> javaTypes() {
        return javaTypes;
    }

    public Set<LogicalType> logicalTypes() {
        return logicalTypes;
    }

    public Set<String> requiredCapabilities() {
        return requiredCapabilities;
    }

    /** 接口或父类声明也覆盖其具体实现类型。 */
    public boolean supportsJavaType(Class<?> javaType) {
        Class<?> candidate = Objects.requireNonNull(javaType, "value codec Java type must not be null");
        for (Class<?> supported : javaTypes) {
            if (supported.isAssignableFrom(candidate)) {
                return true;
            }
        }
        return false;
    }

    public boolean supportsLogicalType(LogicalType logicalType) {
        return logicalTypes.contains(Objects.requireNonNull(
                logicalType, "value codec logical type must not be null"));
    }

    public boolean requiresCapability(String capability) {
        return requiredCapabilities.contains(Names.key(capability, "value codec capability"));
    }

    public String fingerprint() {
        return fingerprint;
    }

    private String computeFingerprint() {
        List<Class<?>> orderedJavaTypes = javaTypes.stream()
                .sorted(Comparator.comparing(Class::getName)).toList();
        List<LogicalType> orderedLogicalTypes = logicalTypes.stream()
                .sorted(Comparator.comparing(Enum::name)).toList();
        List<String> orderedCapabilities = requiredCapabilities.stream().sorted().toList();
        StableEncoder encoder = StableDigest.sha256(FINGERPRINT_DOMAIN)
                                            .text("ID", id)
                                            .integer("JAVA_TYPE_COUNT", orderedJavaTypes.size());
        for (Class<?> javaType : orderedJavaTypes) {
            encoder.text("JAVA_TYPE", javaType.getName());
        }
        encoder.integer("LOGICAL_TYPE_COUNT", orderedLogicalTypes.size());
        for (LogicalType logicalType : orderedLogicalTypes) {
            encoder.text("LOGICAL_TYPE", logicalType.name());
        }
        encoder.integer("CAPABILITY_COUNT", orderedCapabilities.size());
        for (String capability : orderedCapabilities) {
            encoder.text("CAPABILITY", capability);
        }
        return encoder.finishHex();
    }

    private static Set<Class<?>> copyTypes(Collection<Class<?>> types) {
        Collection<Class<?>> source = Objects.requireNonNull(types, "value codec Java types must not be null");
        java.util.LinkedHashSet<Class<?>> copied = new java.util.LinkedHashSet<>();
        for (Class<?> type : source) {
            copied.add(Objects.requireNonNull(type, "value codec Java type must not be null"));
        }
        return Set.copyOf(copied);
    }

    private static Set<LogicalType> copyLogicalTypes(Collection<LogicalType> types) {
        Collection<LogicalType> source = Objects.requireNonNull(
                types, "value codec logical types must not be null");
        java.util.EnumSet<LogicalType> copied = java.util.EnumSet.noneOf(LogicalType.class);
        for (LogicalType type : source) {
            copied.add(Objects.requireNonNull(type, "value codec logical type must not be null"));
        }
        return copied.isEmpty() ? Set.of() : Set.copyOf(copied);
    }

    private static Set<String> copyCapabilities(Collection<String> capabilities) {
        Collection<String> source = Objects.requireNonNull(
                capabilities, "value codec capabilities must not be null");
        java.util.LinkedHashSet<String> copied = new java.util.LinkedHashSet<>();
        for (String capability : source) {
            copied.add(Names.key(capability, "value codec capability"));
        }
        return copied.isEmpty() ? Set.of() : Set.copyOf(copied);
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof ValueCodecDescriptor that
                && id.equals(that.id)
                && javaTypes.equals(that.javaTypes)
                && logicalTypes.equals(that.logicalTypes)
                && requiredCapabilities.equals(that.requiredCapabilities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, javaTypes, logicalTypes, requiredCapabilities);
    }

    @Override
    public String toString() {
        return "ValueCodecDescriptor[id=" + id + ", javaTypes=" + javaTypes
                + ", logicalTypes=" + logicalTypes + ", capabilities=" + requiredCapabilities + ']';
    }
}
