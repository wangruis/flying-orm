package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;

import java.util.List;

/**
 * 可信后端的动态表单 DDL 渲染扩展；主表和检索辅助表分别调用，再合并执行顺序。
 *
 * <p>接收内置规划器已允许的物理表计划，不替代结构规划或执行。实现必须保留计划的
 * 结构、依赖、跳过项及显式并发索引语义；不能借此执行额外结构变更。回调应无状态、可并发调用，
 * 不接收前端原生 SQL。内置规划器不支持的结构仍应使用完整关系 Schema 入口。</p>
 *
 * <p>空计划不调用扩展，非空计划必须返回非空请求。独立的 createIndexes 等低层 SQL 工具不经过此入口。</p>
 *
 * @author wangr
 * @version v4.1.2
 */
@FunctionalInterface
public interface FormSchemaDdlRenderer {

    /**
     * 返回本表的完整 SQL，含计划所需的序列、索引及注释，顺序由实现保持。
     * @param current 已观察的表；新表为 null
     * @param snapshot 本次规划读取的物理快照；未读取时为 null，不应据此猜测物理属性
     * @param plan 内置计划；目标使用规范表定义，已应用本次并发索引选项
     * @return 本表 SQL 请求
     */
    List<SqlRequest> render(RelationalTableDefinition current, SchemaSnapshot snapshot, SchemaMigrationPlan plan);

}
