package com.flying.orm.rdb.metadata;

/**
 * 带本地缓存能力的响应式元数据读取器。
 *
 * <p>使用方只依赖这个契约，不需要知道底层是不是 Caffeine。读取、精确失效和统计快照放在一起，
 * 既方便动态改表后立即清缓存，也方便把基础指标交给外部监控系统。</p>
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public interface ReactiveFormMetadataCache extends ReactiveFormMetadataReader, MetadataCacheInvalidator {

    /**
     * 清掉指定物理表的缓存。这里明确重声明一次，是为了保证缓存 reader 必须真的执行失效，
     * 不能沿用普通元数据 reader 的空操作。
     *
     * @param table 物理表名，可以是 table 或 schema.table
     */
    @Override
    void invalidate(String table);

    /**
     * 读取当前缓存快照。返回值只包含不可变基础数据，不会泄露具体缓存实现。
     *
     * @return 表单和物理表两个缓存分区的统计快照
     */
    MetadataCacheSnapshot snapshot();
}
