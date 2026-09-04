package com.flying.orm.rdb.dialect;

import com.flying.orm.core.internal.hash.StableDigest;
import com.flying.orm.core.internal.hash.StableEncoder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 一份构造后不再变化的方言能力快照。
 *
 * <p>能力判断直接查询构造时冻结的集合；稳定指纹也只计算一次。普通 SQL 渲染不会排序、复制，
 * 也不会根据数据库名称或版本临时推断能力。空快照表示“没有已确认事实”，因此未知数据库版本
 * 和旧自定义方言都会自然地 fail closed。</p>
 *
 * @author wangr
 * @date 2026-09-03
 * @version v3.2
 */
public final class DialectCapabilities {

    private static final StableDigest.Domain FINGERPRINT_DOMAIN =
            StableDigest.domain("dialect-capabilities/v1");
    private static final DialectCapabilities EMPTY = new DialectCapabilities(List.of());

    private final Set<DialectCapabilityId> ids;
    private final String fingerprint;

    private DialectCapabilities(Collection<DialectCapabilityId> ids) {
        this.ids = Set.copyOf(Objects.requireNonNull(ids, "dialect capabilities must not be null"));
        this.fingerprint = fingerprint(this.ids);
    }

    /** @return 没有任何已确认能力的共享快照 */
    public static DialectCapabilities empty() {
        return EMPTY;
    }

    /**
     * 从稳定 ID 创建只读快照。声明顺序和重复项不影响结果或指纹。
     *
     * @param ids 已确认支持的能力
     * @return 不可变能力快照
     */
    public static DialectCapabilities of(DialectCapabilityId... ids) {
        DialectCapabilityId[] source = Objects.requireNonNull(ids, "dialect capabilities must not be null");
        return source.length == 0 ? EMPTY : new DialectCapabilities(List.of(source));
    }

    /**
     * 复制一组动态收集的能力 ID。调用方随后修改原集合不会影响快照。
     */
    public static DialectCapabilities copyOf(Collection<DialectCapabilityId> ids) {
        Collection<DialectCapabilityId> source = Objects.requireNonNull(
                ids, "dialect capabilities must not be null");
        return source.isEmpty() ? EMPTY : new DialectCapabilities(source);
    }

    static DialectCapabilities from(Set<DialectFeature> features) {
        Set<DialectFeature> source = Objects.requireNonNull(features, "dialect features must not be null");
        if (source.isEmpty()) {
            return EMPTY;
        }
        List<DialectCapabilityId> ids = new ArrayList<>(source.size());
        for (DialectFeature feature : source) {
            ids.add(DialectCapabilityId.from(feature));
        }
        return new DialectCapabilities(ids);
    }

    /**
     * 判断这份已解析快照是否明确支持某项能力。false 表示“不承诺”，不是厂商能力声明。
     */
    public boolean supports(DialectCapabilityId id) {
        return ids.contains(Objects.requireNonNull(id, "dialect capability id must not be null"));
    }

    /** @return 已确认能力的不可变集合 */
    public Set<DialectCapabilityId> ids() {
        return ids;
    }

    /** @return 与声明顺序无关的 64 个小写十六进制字符 */
    public String fingerprint() {
        return fingerprint;
    }

    private static String fingerprint(Set<DialectCapabilityId> ids) {
        List<DialectCapabilityId> ordered = ids.stream().sorted().toList();
        StableEncoder encoder = StableDigest.sha256(FINGERPRINT_DOMAIN)
                                            .integer("CAPABILITY_COUNT", ordered.size());
        for (DialectCapabilityId id : ordered) {
            encoder.text("CAPABILITY", id.value());
        }
        return encoder.finishHex();
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof DialectCapabilities that && ids.equals(that.ids);
    }

    @Override
    public int hashCode() {
        return ids.hashCode();
    }

    @Override
    public String toString() {
        return ids.toString();
    }
}
