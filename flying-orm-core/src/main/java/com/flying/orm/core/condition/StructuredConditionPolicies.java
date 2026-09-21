package com.flying.orm.core.condition;

import java.util.Collection;

/**
 * 常用前端条件策略放这里。调用方可以先拿一个预设，再按自己的业务继续收口。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public final class StructuredConditionPolicies {

    private StructuredConditionPolicies() {
    }

    /**
     * 公开 API 用这个更稳：条件树浅一点、节点少一点、字符串短一点。
     *
     * @return 公开 API 策略
     */
    public static StructuredConditionPolicy publicApi() {
        return StructuredConditionPolicy.defaults()
                                        .withMaxDepth(4)
                                        .withMaxNodes(32)
                                        .withMaxCollectionSize(100)
                                        .withMaxStringLength(512);
    }

    /**
     * 动态表单默认策略，比公开 API 宽一点，适合普通后台列表查询。
     *
     * @return 动态表单策略
     */
    public static StructuredConditionPolicy dynamicForm() {
        return StructuredConditionPolicy.defaults()
                                        .withMaxDepth(6)
                                        .withMaxNodes(64)
                                        .withMaxCollectionSize(500)
                                        .withMaxStringLength(2_048);
    }

    /**
     * 管理端策略，保留默认上限，适合受信任后台页面。
     *
     * @return 管理端策略
     */
    public static StructuredConditionPolicy adminConsole() {
        return StructuredConditionPolicy.defaults();
    }

    /**
     * 公开 API 常见写法：先限定字段，再用较紧的查询上限。
     *
     * @param fields 前端能查的字段
     * @return 公开 API 策略
     */
    public static StructuredConditionPolicy publicApi(Collection<String> fields) {
        return publicApi().allowOnlyFields(fields);
    }

    /**
     * 动态表单常见写法：先限定字段，再使用普通后台列表查询上限。
     *
     * @param fields 前端能查的字段
     * @return 动态表单策略
     */
    public static StructuredConditionPolicy dynamicForm(Collection<String> fields) {
        return dynamicForm().allowOnlyFields(fields);
    }

    /**
     * 管理端常见写法：限定字段，但保留默认树深、节点和集合上限。
     *
     * @param fields 前端能查的字段
     * @return 管理端策略
     */
    public static StructuredConditionPolicy adminConsole(Collection<String> fields) {
        return adminConsole().allowOnlyFields(fields);
    }
}
