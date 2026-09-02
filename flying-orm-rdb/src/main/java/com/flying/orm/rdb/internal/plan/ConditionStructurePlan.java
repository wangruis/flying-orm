package com.flying.orm.rdb.internal.plan;

import com.flying.orm.core.internal.value.OwnedBindableValues;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 一次请求对结构计划的执行视图。{@link #plan()} 可以跨请求共享，{@link #parameters()} 只属于当前请求。
 *
 * @param plan 共享条件计划
 * @param parameters 当前请求按槽位排列的参数快照
 * @param shape 不含参数值的稳定条件形状；不可缓存时为空字符串
 * @param cacheable 是否能够安全进入跨请求结构缓存
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record ConditionStructurePlan(ConditionPlan plan,
                                     List<Object> parameters,
                                     String shape,
                                     boolean cacheable) {

    /** 复制参数数组，禁止调用方修改后污染正在执行的请求。 */
    public ConditionStructurePlan {
        plan = Objects.requireNonNull(plan, "condition plan must not be null");
        List<Object> safeParameters = Objects.requireNonNull(
                parameters, "condition plan parameters must not be null");
        parameters = OwnedBindableValues.isPublished(safeParameters)
                ? safeParameters
                : Collections.unmodifiableList(new ArrayList<>(safeParameters));
        shape = Objects.requireNonNull(shape, "condition plan shape must not be null");
        if (parameters.size() != plan.parameterCount()) {
            throw new IllegalArgumentException("condition parameter count does not match compiled plan");
        }
        if (cacheable && shape.isBlank()) {
            throw new IllegalArgumentException("cacheable condition plan must have a stable shape");
        }
    }
}
