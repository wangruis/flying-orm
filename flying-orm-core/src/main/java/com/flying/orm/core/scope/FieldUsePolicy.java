package com.flying.orm.core.scope;

import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.join.JoinFieldRef;
import com.flying.orm.core.join.JoinSource;

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

    private static final FieldUsePolicy UNRESTRICTED =
            new FieldUsePolicy(Map.of(), Map.of(), true);

    private final Map<GrantKey, Set<FieldUse>> grants;
    private final Map<JoinGrantKey, Set<FieldUse>> joinGrants;
    private final boolean unrestricted;

    private FieldUsePolicy(Map<GrantKey, Set<FieldUse>> grants,
                           Map<JoinGrantKey, Set<FieldUse>> joinGrants,
                           boolean unrestricted) {
        this.grants = grants;
        this.joinGrants = joinGrants;
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

    /**
     * 用每项要求所属数据源自己的 FieldScope 审批 governed JOIN。
     *
     * <p>多源 JOIN 只读取来源限定授权；单源 JOIN 兼容已有裸字段策略，但发布的决定仍带来源身份。</p>
     */
    public FieldUseSnapshot approveJoin(FieldUseRequirements requirements,
                                        Map<JoinSource, FieldScope> scopes) {
        FieldUseRequirements required = Objects.requireNonNull(
                requirements, "field use requirements must not be null");
        if (!required.requirements().isEmpty()) {
            throw new IllegalArgumentException(
                    "join approval requires source-qualified field requirements");
        }
        Map<JoinSource, FieldScope> safeScopes = Objects.requireNonNull(
                scopes, "join field scopes must not be null");
        boolean allScopesUnrestricted = true;
        for (FieldScope scope : safeScopes.values()) {
            FieldScope safeScope = Objects.requireNonNull(scope, "join field scope must not be null");
            if (!safeScope.unrestrictedRead() || !safeScope.unrestrictedWrite()) {
                allScopesUnrestricted = false;
                break;
            }
        }
        if (unrestricted && allScopesUnrestricted) {
            return FieldUseSnapshot.unrestricted();
        }
        boolean allowBareCompatibility = safeScopes.size() == 1;
        List<JoinFieldDecision> decisions = new ArrayList<>(required.joinRequirements().size());
        for (FieldUseRequirements.JoinRequirement requirement : required.joinRequirements()) {
            FieldScope scope = safeScopes.get(requirement.field().source());
            if (scope == null) {
                throw new IllegalArgumentException(
                        "join field scope is missing for source ["
                                + requirement.field().source().ordinal() + "]");
            }
            decisions.add(decideJoin(requirement.field(), requirement.use(), requirement.origin(),
                                     scope, allowBareCompatibility));
        }
        return FieldUseSnapshot.ofJoin(decisions);
    }

    /** 对一项来源限定用途应用 qualified policy、origin 隔离和该来源 FieldScope。 */
    public JoinFieldDecision decideJoin(JoinFieldRef field,
                                        FieldUse use,
                                        FieldUseOrigin origin,
                                        FieldScope scope) {
        return decideJoin(field, use, origin, scope, false);
    }

    private JoinFieldDecision decideJoin(JoinFieldRef field,
                                         FieldUse use,
                                         FieldUseOrigin origin,
                                         FieldScope scope,
                                         boolean allowBareCompatibility) {
        JoinFieldRef safeField = Objects.requireNonNull(field, "join field reference must not be null");
        FieldUse safeUse = Objects.requireNonNull(use, "field use must not be null");
        FieldUseOrigin safeOrigin = Objects.requireNonNull(origin, "field use origin must not be null");
        FieldScope safeScope = Objects.requireNonNull(scope, "field scope must not be null");
        boolean policyAllows = unrestricted
                || allowsJoin(safeField, safeOrigin, safeUse)
                || (allowBareCompatibility && allows(safeField.field(), safeOrigin, safeUse));
        boolean scopeAllows = safeOrigin.internal()
                || (safeUse.write()
                        ? safeScope.canWrite(safeField.field())
                        : safeScope.canRead(safeField.field()));
        boolean allowed = policyAllows && scopeAllows;
        FieldVisibility visibility = allowed
                ? joinVisibility(safeField, safeOrigin, allowBareCompatibility)
                : FieldVisibility.HIDDEN;
        return new JoinFieldDecision(
                safeField, safeUse, safeOrigin, allowed, visibility);
    }

    private boolean allows(String field, FieldUseOrigin origin, FieldUse use) {
        Set<FieldUse> uses = grants.get(new GrantKey(field, origin));
        return uses != null && uses.contains(use);
    }

    private boolean allowsJoin(JoinFieldRef field, FieldUseOrigin origin, FieldUse use) {
        Set<FieldUse> uses = joinGrants.get(new JoinGrantKey(field, origin));
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

    private FieldVisibility joinVisibility(JoinFieldRef field,
                                           FieldUseOrigin origin,
                                           boolean allowBareCompatibility) {
        if (origin.internal()) {
            return FieldVisibility.HIDDEN;
        }
        if (unrestricted) {
            return FieldVisibility.FULL;
        }
        Set<FieldUse> uses = joinGrants.get(new JoinGrantKey(field, FieldUseOrigin.CALLER));
        if (uses == null && allowBareCompatibility) {
            uses = grants.get(new GrantKey(field.field(), FieldUseOrigin.CALLER));
        }
        return visibility(uses);
    }

    private static FieldVisibility visibility(Set<FieldUse> uses) {
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

    private record JoinGrantKey(JoinFieldRef field, FieldUseOrigin origin) {

        private JoinGrantKey {
            field = Objects.requireNonNull(field, "join field reference must not be null");
            origin = Objects.requireNonNull(origin, "field use origin must not be null");
        }
    }

    /** 单线程配置期 builder；build 会深复制所有 EnumSet。 */
    public static final class Builder {

        private final Map<GrantKey, EnumSet<FieldUse>> grants = new LinkedHashMap<>();
        private Map<JoinGrantKey, EnumSet<FieldUse>> joinGrants;

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

        public Builder allowJoin(JoinFieldRef field, FieldUse first, FieldUse... rest) {
            return allowJoin(field, FieldUseOrigin.CALLER, first, rest);
        }

        public Builder allowJoin(JoinFieldRef field,
                                 FieldUseOrigin origin,
                                 FieldUse first,
                                 FieldUse... rest) {
            JoinGrantKey key = new JoinGrantKey(field, origin);
            if (joinGrants == null) {
                joinGrants = new LinkedHashMap<>();
            }
            EnumSet<FieldUse> uses = joinGrants.computeIfAbsent(
                    key, ignored -> EnumSet.noneOf(FieldUse.class));
            uses.add(Objects.requireNonNull(first, "field use must not be null"));
            if (rest != null) {
                for (FieldUse use : rest) {
                    uses.add(Objects.requireNonNull(use, "field use must not be null"));
                }
            }
            return this;
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

        /** 设置一个来源限定 JOIN 字段的 caller 结果显示上限。 */
        public Builder joinVisibility(JoinFieldRef field, FieldVisibility visibility) {
            JoinGrantKey key = new JoinGrantKey(field, FieldUseOrigin.CALLER);
            if (joinGrants == null) {
                joinGrants = new LinkedHashMap<>();
            }
            EnumSet<FieldUse> uses = joinGrants.computeIfAbsent(
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
            boolean noJoinGrants = joinGrants == null || joinGrants.isEmpty();
            if (grants.isEmpty() && noJoinGrants) {
                return new FieldUsePolicy(Map.of(), Map.of(), false);
            }
            Map<GrantKey, Set<FieldUse>> frozen = new LinkedHashMap<>();
            grants.forEach((key, uses) -> frozen.put(key, Set.copyOf(uses)));
            Map<JoinGrantKey, Set<FieldUse>> frozenJoin;
            if (noJoinGrants) {
                frozenJoin = Map.of();
            } else {
                Map<JoinGrantKey, Set<FieldUse>> values = new LinkedHashMap<>();
                joinGrants.forEach((key, uses) -> values.put(key, Set.copyOf(uses)));
                frozenJoin = Map.copyOf(values);
            }
            return new FieldUsePolicy(Map.copyOf(frozen), frozenJoin, false);
        }
    }
}
