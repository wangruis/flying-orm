package com.flying.orm.rdb.form;

import com.flying.orm.core.scope.FieldUseSnapshot;

import java.util.Objects;

/**
 * 受治理调用的一次性计划封装。
 *
 * <p>内部计划仍是既有计划类型，SQL 执行器不会看到第二套请求模型。该封装只保存本次重新审批得到的
 * 字段用途快照，绝不进入结构缓存；legacy unrestricted/default 路径不会创建它。</p>
 */
record GovernedPlanEnvelope<T>(T plan, FieldUseSnapshot fieldUse) {

    GovernedPlanEnvelope {
        plan = Objects.requireNonNull(plan, "governed plan must not be null");
        fieldUse = Objects.requireNonNull(fieldUse, "field use snapshot must not be null");
    }
}
