package com.flying.orm.rdb.repository;

/**
 * Repository 批量写入是否需要安装生命周期状态。
 *
 * <p>没有监听器且不需要回填数据库生成键时使用共享 {@link #NOOP}，同步和响应式入口都直接把
 * 映射后的行交给表单批量执行器；只有确实需要提交后回调或生成键回填时才创建有界 tracker。</p>
 */
enum RepositoryBatchLifecyclePlan {

    NOOP,
    TRACKED;

    static RepositoryBatchLifecyclePlan select(boolean lifecycleWork, boolean generatedKeys) {
        return lifecycleWork || generatedKeys ? TRACKED : NOOP;
    }

    boolean tracked() {
        return this == TRACKED;
    }
}
