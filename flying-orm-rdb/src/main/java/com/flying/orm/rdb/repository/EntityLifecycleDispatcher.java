package com.flying.orm.rdb.repository;

import com.flying.orm.rdb.lifecycle.CommittedEntityLifecycleException;
import com.flying.orm.rdb.lifecycle.EntityLifecycleEvent;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.lifecycle.ReactiveEntityListener;
import com.flying.orm.rdb.mapping.EntityMetadata;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 把实体写入阶段交给 flying-orm 自己的响应式监听器。
 *
 * <p>V2 不再扫描 Jakarta Persistence 回调注解。需要审计、补值或联动业务时，调用方显式注册
 * {@link ReactiveEntityListener}，同一套监听器可以同时用于 JDBC 和 R2DBC Repository。没有监听器时
 * 直接返回空完成，不做反射扫描，也不会为了不存在的回调保留批量实体。</p>
 */
final class EntityLifecycleDispatcher<T> {

    private final EntityMetadata<T> metadata;

    private final ReactiveEntityListener<T> listener;

    private final boolean listenerConfigured;

    EntityLifecycleDispatcher(EntityMetadata<T> metadata, ReactiveEntityListener<T> listener) {
        this.metadata = Objects.requireNonNull(metadata, "entity lifecycle metadata must not be null");
        this.listenerConfigured = listener != null;
        this.listener = listenerConfigured ? listener : ReactiveEntityListener.none();
    }

    Mono<Void> fire(EntityLifecyclePhase phase, T entity, Object result) {
        return fire(phase, entity, result, true);
    }

    /** 分发一次生命周期事件；只有已经确认提交的 POST 失败才转换为提交后回调异常。 */
    Mono<Void> fire(EntityLifecyclePhase phase, T entity, Object result, boolean committed) {
        if (!listenerConfigured) {
            return Mono.empty();
        }
        EntityLifecyclePhase safePhase = Objects.requireNonNull(
                phase, "entity lifecycle phase must not be null");
        // defer 保证只创建执行链时不会提前改实体，也不会提前触发审计或业务联动。
        Mono<Void> callback = Mono.defer(() -> {
            EntityLifecycleEvent<T> event = new EntityLifecycleEvent<>(metadata, entity, safePhase, result);
            Publisher<Void> completion = Objects.requireNonNull(
                    listener.onEvent(event), "entity lifecycle listener must return a Publisher");
            return Mono.from(completion);
        });
        if (!committed || (safePhase != EntityLifecyclePhase.POST_PERSIST
                && safePhase != EntityLifecyclePhase.POST_UPDATE
                && safePhase != EntityLifecyclePhase.POST_REMOVE)) {
            return callback;
        }
        return callback.onErrorMap(error -> {
            Throwable preferred = RepositoryFailureSupport.preferVirtualMachineError(error);
            return preferred instanceof VirtualMachineError fatal
                    ? fatal
                    : error instanceof CommittedEntityLifecycleException
                    ? error
                    : new CommittedEntityLifecycleException(safePhase, result, error);
        });
    }

    /** 没有监听器就没有生命周期工作，批量路径也不需要暂存实体引用。 */
    boolean hasWork(EntityLifecyclePhase phase) {
        Objects.requireNonNull(phase, "entity lifecycle phase must not be null");
        return listenerConfigured;
    }
}
