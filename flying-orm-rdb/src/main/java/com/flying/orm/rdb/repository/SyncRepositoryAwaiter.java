package com.flying.orm.rdb.repository;

import com.flying.orm.rdb.internal.sync.SyncBlockingGuard;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Objects;

/**
 * 同步 Repository 等待实体生命周期 Mono 的唯一位置。
 *
 * <p>SQL、连接、事务和批量处理都走原生 JDBC；只有兼容两种执行方式的实体监听器仍返回 Mono。本类集中检查
 * 当前线程并按统一上限等待监听器，不能被用来把 R2DBC 数据库操作伪装成同步调用。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
final class SyncRepositoryAwaiter {

    private final Duration timeout;

    SyncRepositoryAwaiter(Duration timeout) {
        this.timeout = SyncBlockingGuard.requirePositiveTimeout(timeout, "sync repository timeout");
    }

    /**
     * 生命周期监听器只需要确认完成，不应该伪装成必须产出值的查询结果。这个方法把允许空完成的等待
     * 集中在同步 Repository 边界，普通 JDBC CRUD 没有监听器时不会走到这里。
     */
    void awaitCompletion(Mono<Void> operation) {
        SyncBlockingGuard.nullable(Objects.requireNonNull(operation, "sync repository completion must not be null"),
                                 timeout);
    }

    /**
     * 在同步入口读取 List 或创建 Publisher 前先做线程检查，避免在 event-loop 上提前遍历大型集合。
     */
    void rejectNonBlockingThread() {
        SyncBlockingGuard.rejectNonBlockingThread();
    }
}
