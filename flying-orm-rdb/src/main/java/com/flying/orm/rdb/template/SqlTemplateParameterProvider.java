package com.flying.orm.rdb.template;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Set;

/**
 * 在 SQL 模板真正被订阅时提供租户、用户等可信服务端参数。
 *
 * <p>实现可以使用 {@code Mono.deferContextual(...)} 从 Reactor Context 读取请求范围，也可以从上层已经验证过的
 * 上下文中返回值。返回结果必须只包含 {@code parameterNames} 指定的名称，不能夹带普通业务参数。调用方通过
 * {@code bind(...)} 无法覆盖这些值。</p>
 *
 * <p>提供器本身应当无阻塞、可并发共享，不得在这里获取数据库连接。flying-orm 会在执行 SQL 前检查缺失和多余
 * 参数，检查失败时不会调用底层执行器。</p>
 *
 * @author wangr
 * @date 2026-08-04
 * @version v1.0
 */
@FunctionalInterface
public interface SqlTemplateParameterProvider {

    /**
     * 读取一次模板执行需要的可信参数。
     *
     * @param templateId 稳定模板 ID
     * @param parameterNames 模板声明的全部服务端参数名，不可修改
     * @return 只发出一个参数 Map 的 Publisher；参数值允许为 null
     */
    Publisher<? extends Map<String, ?>> parameters(String templateId, Set<String> parameterNames);

    /** 没有服务端参数时使用的默认实现。声明了安全参数的模板会因为缺失值而在执行前失败。 */
    static SqlTemplateParameterProvider none() {
        return (templateId, parameterNames) -> Mono.just(Map.of());
    }
}
