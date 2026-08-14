package com.flying.orm.rdb.vector;

import java.util.Objects;

/** 条件渲染前已经校验完成的向量和阈值，避免 SQL handler 再猜前端 Map 结构。 */
record VectorConditionValue(float[] vector, double threshold, VectorMetric metric) {

    VectorConditionValue {
        vector = Objects.requireNonNull(vector, "condition vector must not be null").clone();
        metric = Objects.requireNonNull(metric, "vector metric must not be null");
        if (!Double.isFinite(threshold)) {
            throw new IllegalArgumentException("vector distance threshold must be finite");
        }
    }

    @Override
    public float[] vector() {
        return vector.clone();
    }
}
