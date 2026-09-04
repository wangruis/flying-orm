package com.flying.orm.core.scope;

import com.flying.orm.core.field.FieldIdentity;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 当前一次调用的字段用途批准快照。
 *
 * <p>普通 legacy 路径使用共享的 {@link #unrestricted()}，不创建 decision 列表。受治理路径发布
 * 独立不可变列表，调用方不能修改它，也不能把这份带动态权限的结果放进跨调用结构缓存。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class FieldUseSnapshot {

    private static final FieldUseSnapshot UNRESTRICTED =
            new FieldUseSnapshot(List.of(), true);

    private final List<FieldDecision> decisions;
    private final boolean unrestricted;
    private final boolean allowed;
    private final Map<String, FieldVisibility> callerVisibility;

    private FieldUseSnapshot(List<FieldDecision> decisions, boolean unrestricted) {
        this.decisions = List.copyOf(Objects.requireNonNull(
                decisions, "field decisions must not be null"));
        this.unrestricted = unrestricted;
        boolean allAllowed = true;
        Map<String, FieldVisibility> compiled = unrestricted ? null : new HashMap<>();
        for (FieldDecision decision : this.decisions) {
            allAllowed &= decision.allowed();
            if (compiled == null
                    || !decision.allowed()
                    || decision.origin() != FieldUseOrigin.CALLER
                    || decision.visibility() == FieldVisibility.HIDDEN) {
                continue;
            }
            compiled.merge(decision.field(), decision.visibility(),
                           FieldUseSnapshot::mostVisible);
        }
        this.allowed = unrestricted || allAllowed;
        this.callerVisibility = compiled == null ? Map.of() : Map.copyOf(compiled);
    }

    /** @return 旧入口和显式 unrestricted preview 共用的零分配快照 */
    public static FieldUseSnapshot unrestricted() {
        return UNRESTRICTED;
    }

    /** 创建一次受治理调用的不可变 decision 快照。 */
    public static FieldUseSnapshot of(List<FieldDecision> decisions) {
        return new FieldUseSnapshot(decisions, false);
    }

    public List<FieldDecision> decisions() {
        return decisions;
    }

    public List<FieldDecision> deniedDecisions() {
        return decisions.stream().filter(FieldDecision::denied).toList();
    }

    /** @return 所有要求是否都获准 */
    public boolean allowed() {
        return allowed;
    }

    public boolean isUnrestricted() {
        return unrestricted;
    }

    /** 查找一项精确到 origin 的决定，避免把内部批准误当成 caller 批准。 */
    public Optional<FieldDecision> decision(String field,
                                            FieldUse use,
                                            FieldUseOrigin origin) {
        String key = FieldIdentity.of(field).key();
        FieldUse safeUse = Objects.requireNonNull(use, "field use must not be null");
        FieldUseOrigin safeOrigin = Objects.requireNonNull(origin, "field use origin must not be null");
        if (unrestricted) {
            FieldVisibility visibility = safeOrigin.internal()
                    ? FieldVisibility.HIDDEN : FieldVisibility.FULL;
            return Optional.of(new FieldDecision(key, safeUse, safeOrigin, true, visibility));
        }
        return decisions.stream()
                .filter(value -> value.field().equals(key)
                        && value.use() == safeUse
                        && value.origin() == safeOrigin)
                .findFirst();
    }

    /** 返回 caller 字段在本次快照中的最高显示级别；构造时已合并完成，没有批准时为 HIDDEN。 */
    public FieldVisibility visibility(String field) {
        if (unrestricted) {
            return FieldVisibility.FULL;
        }
        String key = FieldIdentity.of(field).key();
        return callerVisibility.getOrDefault(key, FieldVisibility.HIDDEN);
    }

    private static FieldVisibility mostVisible(FieldVisibility left, FieldVisibility right) {
        return left == FieldVisibility.FULL || right == FieldVisibility.FULL
                ? FieldVisibility.FULL : FieldVisibility.MASKED;
    }
}
