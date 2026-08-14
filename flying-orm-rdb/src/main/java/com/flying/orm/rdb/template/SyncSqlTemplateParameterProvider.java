package com.flying.orm.rdb.template;

import java.util.Map;
import java.util.Set;

/**
 * 同步模板执行前提供可信服务端参数的最小契约。
 *
 * <p>它专门给原生 JDBC 同步链路使用，直接返回本次调用的参数 Map，不接收也不等待响应式 Publisher。
 * 租户、当前用户、数据权限等值应由上层已经验证过的上下文提供；返回的 key 必须与模板登记的
 * {@code parameterNames} 完全相同，flying-orm 会在拿到连接前再次校验。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
@FunctionalInterface
public interface SyncSqlTemplateParameterProvider {

    /**
     * 读取本次模板执行需要的可信参数。
     *
     * @param templateId 稳定模板 ID
     * @param parameterNames 模板声明的全部可信参数名，不可修改
     * @return 仅包含这些参数名的 Map；参数值允许为 null，但 Map 不能为 null
     */
    Map<String, ?> parameters(String templateId, Set<String> parameterNames);

    /** 没有可信参数的模板使用的默认实现。 */
    static SyncSqlTemplateParameterProvider none() {
        return (templateId, parameterNames) -> Map.of();
    }
}
