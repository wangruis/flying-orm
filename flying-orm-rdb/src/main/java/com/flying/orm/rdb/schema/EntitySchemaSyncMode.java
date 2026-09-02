package com.flying.orm.rdb.schema;

/**
 * 实体表结构在应用启动阶段怎样处理。
 *
 * <p>默认应使用 {@link #OFF}。校验和更新都需要使用方明确开启，破坏性更新还必须带上与审核计划完全匹配的批准指纹。</p>
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

    /** 生成完整审核计划，并且只执行已经用精确计划指纹批准的破坏性变更。 */
    FULL_UPDATE
}
