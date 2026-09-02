package com.flying.orm.rdb.metadata;

/**
 * 元数据缓存失效入口。
 *
 * <p>flying-orm 不会猜表结构什么时候变了。DDL 可能来自本项目，也可能来自别的服务、运维脚本或数据库控制台。
 * 所以缓存必须有明确的手动失效入口，让上层在动态表单变更后主动清掉旧结构。</p>
 *
 * @author wangr
 * @date 2026-07-30
 * @version v1.0
 */
public interface MetadataCacheInvalidator {

    /**
     * 返回不做联动失效的实现。只适用于没有下游计划缓存的独立元数据缓存，避免调用方为可选协作者写样板类。
     *
     * @return 可并发共享的无操作失效器
     */
    static MetadataCacheInvalidator none() {
        return NoopMetadataCacheInvalidator.INSTANCE;
    }

    /**
     * 清掉指定物理表的表单和表元数据缓存。这里的 table 要和读取时传入的 table 一致。
     *
     * @param table 物理表名，可以是 table 或 schema.table
     */
    void invalidate(String table);

    /**
     * 清掉指定 schema + table 的表单和表元数据缓存。
     *
     * @param schema 数据库 schema
     * @param table  物理表名
     */
    default void invalidate(String schema, String table) {
        invalidate(schema + "." + table);
    }

    /**
     * 清空所有元数据缓存。适合批量 DDL、应用发布后统一刷新，或者上层懒得精确定位哪张表变了。
     */
    void invalidateAll();

}

/** 包级无状态单例，避免把实现细节扩大为公共 API。 */
enum NoopMetadataCacheInvalidator implements MetadataCacheInvalidator {
    INSTANCE;

    @Override
    public void invalidate(String table) {
        // 明确无下游缓存，不需要动作。
    }

    @Override
    public void invalidateAll() {
        // 明确无下游缓存，不需要动作。
    }
}
