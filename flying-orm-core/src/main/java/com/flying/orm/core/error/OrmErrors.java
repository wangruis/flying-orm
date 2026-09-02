package com.flying.orm.core.error;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Optional;
import java.util.Set;

/**
 * 从异常链里提取 flying-orm 的统一错误报告。响应式库和业务代码经常会包装原异常，上层不需要自己
 * 一层层拆 cause；找不到已知 ORM 错误时返回空，由协议适配器按自己的兜底规则处理。
 *
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class OrmErrors {

    private OrmErrors() {
    }

    /**
     * 从最外层向内查找第一个结构化 ORM 错误。使用对象身份去重，即使第三方异常错误地形成 cause 环也不会死循环。
     *
     * @param error 原始异常或包装异常，允许为 null
     * @return 找到的稳定错误报告；异常链不包含 ORM 错误时为空
     */
    public static Optional<OrmErrorReport> report(Throwable error) {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = error;
        while (current != null && visited.add(current)) {
            if (current instanceof OrmErrorReportProvider provider) {
                return Optional.of(provider.toErrorReport());
            }
            current = current.getCause();
        }
        return Optional.empty();
    }
}
