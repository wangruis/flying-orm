package com.flying.orm.core.scope;

import com.flying.orm.core.field.FieldIdentity;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 每字段、每 origin 的不可变用途白名单。
 *
 * <p>显式 builder 默认拒绝；授权键由规范字段名和 origin 共同组成。CALLER 决定还必须通过
 * {@link FieldScope} 的读写边界，所以策略和 scope 只能求交，任何一边都不能放宽另一边。
 * INTERNAL_* 只服务 ORM 注入结构，始终 HIDDEN，也不会变成 CALLER 权限。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class FieldUsePolicy {

    private static final FieldUsePolicy UNRESTRICTED = new FieldUsePolicy(Map.of(), true);

    private final Map<GrantKey, Set<FieldUse>> grants;
    private final boolean unrestricted;

    private FieldUsePolicy(Map<GrantKey, Set<FieldUse>> grants, boolean unrestricted) {
        this.grants = grants;
        this.unrestricted = unrestricted;
    }

    /** @return 未启用字段治理时复用的静态 singleton */
    public static FieldUsePolicy unrestricted() {
        return UNRESTRICTED;
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean isUnrestricted() {
        return unrestricted;
    }

    /** 对一项用途同时应用 policy、origin 隔离和 FieldScope 交集。 */
    public FieldDecision decide(String field,
                                FieldUse use,
                                FieldUseOrigin origin,
                                FieldScope scope) {
        String key = FieldIdentity.of(field).key();
        FieldUse safeUse = Objects.requireNonNull(use, "field use must not be null");
        FieldUseOrigin safeOrigin = Objects.requireNonNull(origin, "field use origin must not be null");
        FieldScope safeScope = Objects.requireNonNull(scope, "field scope must not be null");

        boolean policyAllows = unrestricted || allows(key, safeOrigin, safeUse);
        // FieldScope 描述 caller 可见/可写字段。内部租户、逻辑删除、版本和 tie-breaker
        // 不向 caller 暴露，因此按独立 origin 授权，而不是借 caller scope 获权。
        boolean scopeAllows = safeOrigin.internal()
                || (safeUse.write() ? safeScope.canWrite(key) : safeScope.canRead(key));
        boolean allowed = policyAllows && scopeAllows;
        FieldVisibility visibility = allowed
                ? visibility(key, safeOrigin)
                : FieldVisibility.HIDDEN;
        return new FieldDecision(key, safeUse, safeOrigin, allowed, visibility);
    }

    /** 用当前调用的 scope 审批结构 requirements，不缓存动态结果。 */
    public FieldUseSnapshot approve(FieldUseRequirements requirements, FieldScope scope) {
        FieldUseRequirements required = Objects.requireNonNull(
                requirements, "field use requirements must not be null");
        FieldScope safeScope = Objects.requireNonNull(scope, "field scope must not be null");
        if (unrestricted && safeScope.unrestrictedRead() && safeScope.unrestrictedWrite()) {
            return FieldUseSnapshot.unrestricted();
        }
        List<FieldDecision> decisions = new ArrayList<>(required.requirements().size());
        for (FieldUseRequirements.Requirement requirement : required.requirements()) {
            decisions.add(decide(requirement.field(), requirement.use(), requirement.origin(), safeScope));
        }
        return FieldUseSnapshot.of(decisions);
    }

    private boolean allows(String field, FieldUseOrigin origin, FieldUse use) {
        Set<FieldUse> uses = grants.get(new GrantKey(field, origin));
        return uses != null && uses.contains(use);
    }

    private FieldVisibility visibility(String field, FieldUseOrigin origin) {
        if (origin.internal()) {
            return FieldVisibility.HIDDEN;
        }
        if (unrestricted) {
            return FieldVisibility.FULL;
        }
        Set<FieldUse> uses = grants.get(new GrantKey(field, FieldUseOrigin.CALLER));
        if (uses == null || !uses.contains(FieldUse.PROJECT)) {
            return FieldVisibility.HIDDEN;
        }
        if (uses.contains(FieldUse.FULL_VALUE)) {
            return FieldVisibility.FULL;
        }
        return uses.contains(FieldUse.MASKED_VALUE)
                ? FieldVisibility.MASKED : FieldVisibility.HIDDEN;
    }

    private record GrantKey(String field, FieldUseOrigin origin) {

        private GrantKey {
            field = FieldIdentity.of(field).key();
            origin = Objects.requireNonNull(origin, "field use origin must not be null");
        }
    }

    /** 单线程配置期 builder；build 会深复制所有 EnumSet。 */
    public static final class Builder {

        private final Map<GrantKey, EnumSet<FieldUse>> grants = new LinkedHashMap<>();

        public Builder allow(String field, FieldUse first, FieldUse... rest) {
            return allow(field, FieldUseOrigin.CALLER, first, rest);
        }

        public Builder allow(String field,
                             FieldUseOrigin origin,
                             FieldUse first,
                             FieldUse... rest) {
            GrantKey key = new GrantKey(field, origin);
            EnumSet<FieldUse> uses = grants.computeIfAbsent(
                    key, ignored -> EnumSet.noneOf(FieldUse.class));
            uses.add(Objects.requireNonNull(first, "field use must not be null"));
            if (rest != null) {
                for (FieldUse use : rest) {
                    uses.add(Objects.requireNonNull(use, "field use must not be null"));
                }
            }
            return this;
        }

        /** 明确声明内部授权；传 CALLER 是调用错误，不做静默降级。 */
        public Builder allowInternal(String field,
                                     FieldUseOrigin origin,
                                     FieldUse first,
                                     FieldUse... rest) {
            FieldUseOrigin safeOrigin = Objects.requireNonNull(
                    origin, "field use origin must not be null");
            if (!safeOrigin.internal()) {
                throw new IllegalArgumentException("internal field grant requires an INTERNAL_* origin");
            }
            return allow(field, safeOrigin, first, rest);
        }

        /**
         * 设置 caller 结果显示上限。FULL 同时允许 masked 输出；MASKED 不授予明文；HIDDEN
         * 只撤销显示用途，不影响该字段已经显式授予的 FILTER、SORT 或写入用途。
         */
        public Builder visibility(String field, FieldVisibility visibility) {
            GrantKey key = new GrantKey(field, FieldUseOrigin.CALLER);
            EnumSet<FieldUse> uses = grants.computeIfAbsent(
                    key, ignored -> EnumSet.noneOf(FieldUse.class));
            uses.remove(FieldUse.PROJECT);
            uses.remove(FieldUse.FULL_VALUE);
            uses.remove(FieldUse.MASKED_VALUE);
            switch (Objects.requireNonNull(visibility, "field visibility must not be null")) {
                case FULL -> uses.addAll(EnumSet.of(
                        FieldUse.PROJECT, FieldUse.FULL_VALUE, FieldUse.MASKED_VALUE));
                case MASKED -> uses.addAll(EnumSet.of(FieldUse.PROJECT, FieldUse.MASKED_VALUE));
                case HIDDEN -> {
                    // 显示用途已经移除，其他用途保持原样。
                }
            }
            return this;
        }

        public FieldUsePolicy build() {
            if (grants.isEmpty()) {
                return new FieldUsePolicy(Map.of(), false);
            }
            Map<GrantKey, Set<FieldUse>> frozen = new LinkedHashMap<>();
            grants.forEach((key, uses) -> frozen.put(key, Set.copyOf(uses)));
            return new FieldUsePolicy(Map.copyOf(frozen), false);
        }
    }
}
