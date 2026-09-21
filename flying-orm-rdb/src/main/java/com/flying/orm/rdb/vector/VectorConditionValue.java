package com.flying.orm.rdb.vector;

import java.util.Objects;

/** 条件渲染前已经校验完成的向量和阈值，避免 SQL handler 再猜前端 Map 结构。 */
record VectorConditionValue(float[] vector, double threshold, VectorMetric metric) {

    VectorConditionValue {
        vector = Objects.requireNonNull(vector, "condition vector must not be null");
        metric = Objects.requireNonNull(metric, "vector metric must not be null");
        if (!Double.isFinite(threshold)) {
            throw new IllegalArgumentException("vector distance threshold must be finite");
        }
    }

    @Override
    public float[] vector() {
        return vector.clone();
    }

    /** SQL handler 与 codec 位于同一受信任链，复用 codec 新建且已校验的数组。 */
    float[] ownedVector() {
        return vector;
    }
}
