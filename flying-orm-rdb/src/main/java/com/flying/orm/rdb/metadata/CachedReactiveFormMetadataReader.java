package com.flying.orm.rdb.metadata;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.internal.cache.SchemaCacheInvalidationCoordinator;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.schema.SchemaSnapshotCoverage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * 给任意响应式元数据 reader 套一层 Caffeine 本地缓存，降低动态表单高频查询数据库字典的成本。
 *
 * <p>缓存只保存已经读到的表结构，不主动猜数据库有没有变化。DDL 成功后清一下对应表，
 * 读多写少时就少打数据库字典表；结构真变了，也不会靠“等 TTL 到期”硬撑。调用方即使也使用 Caffeine，
 * 两边仍是独立 cache 实例：本类只管理 ORM 元数据，不接管上层业务缓存。</p>
 *
 * <p>缓存值是同一个 key 的共享 Mono，并发 miss 时 Caffeine 只创建一条加载链，所有订阅者共享同一次字典查询。
 * 最后一个等待者取消时底层查询也会取消并逐 key 驱逐；成功结果继续复用，失败结果立即驱逐。</p>
 *
 * @author wangr
 * @date 2026-07-30
 * @version v1.0
 */
final class CachedReactiveFormMetadataReader implements ReactiveFormMetadataCache {

    private final ReactiveFormMetadataReader delegate;

    /** 表单和表结构共享一个权重边界，避免同一 metadata 配置被两块缓存各放大一倍。 */
    private final Cache<MetadataCacheKey, MetadataCachedValue<?>> entries;

    private final CacheRegionPolicy policy;

    private final MetadataCacheInvalidator dependentInvalidator;

    /** Caffeine 只看到 Mono 被放入缓存，真实的响应式成功/失败要在源 Publisher 上单独计数。 */
    private final MetadataCacheRegionStats formStats;

    private final MetadataCacheRegionStats tableStats;

    CachedReactiveFormMetadataReader(ReactiveFormMetadataReader delegate,
                                              CacheRegionPolicy policy,
                                              MetadataCacheInvalidator dependentInvalidator) {
        this.delegate = Objects.requireNonNull(delegate, "reactive form metadata reader must not be null");
        this.policy = Objects.requireNonNull(policy, "metadata cache policy must not be null");
        this.formStats = new MetadataCacheRegionStats(policy.recordStats());
        this.tableStats = new MetadataCacheRegionStats(policy.recordStats());
        this.entries = newCache(policy);
        MetadataCacheInvalidator safeDependent = Objects.requireNonNull(dependentInvalidator,
                "dependent cache invalidator must not be null");
        // 嵌套 reader 的缓存也必须失效，否则下一次读取会把旧结构重新填入本层。
        this.dependentInvalidator = SchemaCacheInvalidationCoordinator.of(delegate, safeDependent);
    }

    @Override
    public SchemaSnapshotCoverage snapshotCoverage() {
        return delegate.snapshotCoverage();
    }

    @Override
    public Flux<String> readTableNames() {
        return Flux.defer(delegate::readTableNames);
    }

    @Override
    public Flux<String> readTableNames(String schema) {
        return Flux.defer(() -> delegate.readTableNames(schema));
    }

    @Override
    public Flux<RelationalTableDefinition> readTables() {
        return Flux.defer(delegate::readTables);
    }

    @Override
    public Flux<RelationalTableDefinition> readTables(String schema) {
        return Flux.defer(() -> delegate.readTables(schema));
    }

    @Override
    public Mono<DynamicForm> readForm(String formId, RelationIdentity relation) {
        MetadataCacheKey key = MetadataCacheKey.form(formId, relation);
        return cached(formStats, key, () -> delegate.readForm(formId, relation));
    }

    @Override
    public Mono<RelationalTableDefinition> readTable(RelationIdentity relation) {
        MetadataCacheKey key = MetadataCacheKey.table(relation);
        return cached(tableStats, key, () -> delegate.readTable(relation));
    }

    @Override
    public Mono<RelationalTableDefinition> readTableForSchema(RelationIdentity relation) {
        return Mono.defer(() -> delegate.readTableForSchema(relation));
    }

    @Override
    public Mono<SchemaSnapshot> readSnapshot(RelationIdentity relation) {
        return Mono.defer(() -> delegate.readSnapshot(relation));
    }

    @Override
    public void invalidate(RelationIdentity relation) {
        RelationIdentity target = Objects.requireNonNull(
                relation, "metadata cache relation must not be null");
        try {
            dependentInvalidator.invalidate(target);
        } finally {
            entries.asMap().keySet().removeIf(key -> key.matchesInvalidation(target));
        }
    }

    @Override
    public void invalidateAll() {
        try {
            dependentInvalidator.invalidateAll();
        } finally {
            entries.invalidateAll();
        }
    }

    /**
     * 生成一份不依赖指标框架的统计快照。先执行 Caffeine 的维护任务，让已经到期或超过容量的条目尽量
     * 在快照前完成清理；entries 仍按 Caffeine 定义属于并发环境下的近似值，命中和加载计数则是累计值。
     *
     * @return forms、tables 两块缓存及其汇总所需的稳定统计
     */
    @Override
    public MetadataCacheSnapshot snapshot() {
        entries.cleanUp();
        long formEntries = 0;
        long tableEntries = 0;
        // Caffeine 的视图是弱一致的。一次遍历里分别计数，避免“表单数来自旧视图、总数来自新视图”算出负数。
        for (MetadataCacheKey key : entries.asMap().keySet()) {
            if (key.kind() == MetadataCacheKey.Kind.FORM) {
                formEntries++;
            } else {
                tableEntries++;
            }
        }
        return new MetadataCacheSnapshot(formStats.snapshot(formEntries), tableStats.snapshot(tableEntries));
    }

    private <T> Mono<T> cached(MetadataCacheRegionStats stats,
                               MetadataCacheKey key,
                               Supplier<Mono<T>> loader) {
        // delegate 可能从当前请求上下文取 schema、租户等信息，必须等真正订阅后再调用。
        // 同步抛出的异常也会由 Mono.defer 变成 onError，不会直接逃出响应式 API。
        if (!policy.enabled()) {
            return Mono.defer(loader);
        }
        return Mono.defer(() -> {
            MetadataCachedValue<?> existing = entries.getIfPresent(key);
            if (existing != null) {
                stats.hit();
                return cast(existing);
            }
            stats.miss();
            return cast(entries.get(key, cacheKey -> load(stats, cacheKey, loader)));
        });
    }

    private <T> MetadataCachedValue<T> load(MetadataCacheRegionStats stats,
                             MetadataCacheKey key,
                             Supplier<Mono<T>> loader) {
        // holder 保存实际放入 cache 的 Mono 引用，remove(key, value) 不会误删并发替换后的新值。
        AtomicReference<MetadataCachedValue<T>> holder = new AtomicReference<>();
        AtomicReference<Mono<T>> valueHolder = new AtomicReference<>();
        long startedAt = System.nanoTime();
        // 先发布缓存占位，再调用 delegate；否则加载链创建期间的失效可能漏掉尚未发布的旧条目。
        Mono<T> source = Mono.defer(loader);
        /*
         * 成功/失败计数必须放在 share() 前面。放在后面时，共享同一次加载的每个订阅者都会各记一次；
         * 放在前面则只统计真正访问 delegate 的源订阅。最后一个等待者取消时，share 会取消源并让占位条目失效，
         * 避免已经没有调用方的元数据查询继续占连接；成功终态仍会向后续订阅者重放。
         */
        Mono<T> value = source.doOnSuccess(result -> {
                                  stats.success(System.nanoTime() - startedAt);
                                  reweigh(key, holder.get(), valueHolder.get(), result);
                              })
                              .doOnError(error -> stats.failure(System.nanoTime() - startedAt))
                              .doOnCancel(() -> entries.asMap().remove(key, holder.get()))
                              .share()
                              .doOnError(error -> entries.asMap().remove(key, holder.get()));
        valueHolder.set(value);
        MetadataCachedValue<T> cachedValue = new MetadataCachedValue<>(value, 1);
        holder.set(cachedValue);
        return cachedValue;
    }

    private void reweigh(MetadataCacheKey key,
                         MetadataCachedValue<?> original,
                         Mono<?> value,
                         Object result) {
        int weight = metadataWeight(result);
        if (weight > policy.maximumEntryWeight()) {
            entries.asMap().remove(key, original);
            return;
        }
        entries.asMap().replace(key, original, new MetadataCachedValue<>(value, weight));
    }

    private Cache<MetadataCacheKey, MetadataCachedValue<?>> newCache(CacheRegionPolicy policy) {
        Caffeine<MetadataCacheKey, MetadataCachedValue<?>> builder = Caffeine.newBuilder()
                                                   .maximumWeight(policy.maximumWeight())
                                                   .weigher((MetadataCacheKey ignored,
                                                             MetadataCachedValue<?> value) -> value.weight())
                                                   .expireAfterAccess(policy.expireAfterAccess())
                                                   // evictionListener 与容量淘汰同步执行，cleanUp 后的快照不会漏掉
                                                   // 尚在异步 removal 通知队列中的淘汰计数。
                                                   .evictionListener((MetadataCacheKey key,
                                                                      MetadataCachedValue<?> value,
                                                                      RemovalCause cause) -> {
                                                       if (key != null && value != null) {
                                                           MetadataCacheRegionStats stats = key.kind() == MetadataCacheKey.Kind.FORM
                                                                   ? formStats : tableStats;
                                                           stats.eviction(value.weight());
                                                       }
                                                   });
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private static <T> Mono<T> cast(MetadataCachedValue<?> value) {
        return (Mono<T>) value.value();
    }

    private static int metadataWeight(Object value) {
        if (value instanceof DynamicForm form) {
            return 1 + form.fields().size();
        }
        if (value instanceof RelationalTableDefinition table) {
            return 1 + table.columns().size() + table.indexes().size() + table.foreignKeys().size();
        }
        return 1;
    }


}
