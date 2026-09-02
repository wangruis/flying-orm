package com.flying.orm.core.internal.condition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 条件 AST 在构造时固定的结构摘要和扁平参数源。
 *
 * @author wangr
 * @version v3.1
 */
public final class ConditionExecutionView {

    private final String shapeDigest;
    private final List<Object> parameterSources;
    private final long requiredStandardTermMask;
    private final boolean cacheable;

    ConditionExecutionView(String shapeDigest,
                           List<Object> parameterSources,
                           long requiredStandardTermMask,
                           boolean cacheable) {
        this.shapeDigest = Objects.requireNonNull(shapeDigest, "condition shape digest must not be null");
        this.parameterSources = Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(
                parameterSources, "condition parameter sources must not be null")));
        this.requiredStandardTermMask = requiredStandardTermMask;
        this.cacheable = cacheable;
    }

    public String shapeDigest() {
        return shapeDigest;
    }

    List<Object> parameterSources() {
        return parameterSources;
    }

    public int parameterCount() {
        return parameterSources.size();
    }

    public boolean cacheable(long rendererStandardTermMask) {
        return cacheable
                && (rendererStandardTermMask & requiredStandardTermMask) == requiredStandardTermMask;
    }

    long requiredStandardTermMask() {
        return requiredStandardTermMask;
    }

    boolean structurallyCacheable() {
        return cacheable;
    }
}
