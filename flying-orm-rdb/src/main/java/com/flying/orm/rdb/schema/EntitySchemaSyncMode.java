package com.flying.orm.rdb.schema;

/**
 * 实体表结构在应用启动阶段怎样处理。
 *
 * <p>默认应使用 {@link #OFF}。校验和更新都需要使用方明确开启，变更范围由所选模式决定。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
public enum EntitySchemaSyncMode {

    /** 不编译实体结构，也不读取数据库元数据。 */
    OFF,

    /** 只比较结构；发现任何需要执行或需要人工处理的差异就失败，不执行 DDL。 */
    VALIDATE,

    /** 只执行现有安全迁移器允许的建表、加列和加索引等动作；发现危险差异时整批拒绝。 */
    SAFE_UPDATE,

    /** 按完整目标结构生成并执行变更，包括调用方明确选择的删列等操作。 */
    FULL_UPDATE
}
