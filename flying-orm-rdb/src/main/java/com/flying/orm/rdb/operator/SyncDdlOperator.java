package com.flying.orm.rdb.operator;

import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.schema.JdbcSchemaClient;

import java.util.Objects;

/**
 * 同步 DDL 入口。
 *
 * <p>该入口直接使用 JDBC Schema 客户端和元数据读取器。元数据读取与 DDL 执行共用上层连接接入和
 * 显式方言，调用方不需要额外拼装执行器。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v2.0
 */
public final class SyncDdlOperator {

    private final JdbcSchemaClient jdbcSchemaClient;
    private final JdbcFormMetadataReader jdbcMetadataReader;

    private SyncDdlOperator(JdbcSchemaClient jdbcSchemaClient, JdbcFormMetadataReader jdbcMetadataReader) {
        this.jdbcSchemaClient = Objects.requireNonNull(jdbcSchemaClient, "jdbc schema client must not be null");
        this.jdbcMetadataReader = Objects.requireNonNull(
                jdbcMetadataReader, "jdbc form metadata reader must not be null");
    }

    /**
     * 创建不会创建响应式运行时的同步 DDL 入口。
     *
     * <p>两个依赖应使用同一上层 JDBC 连接接入和显式方言。连接来源及不同操作间的一致性由上层决定，
     * ORM 不探测或参与事务。</p>
     *
     * @param schemaClient 原生 JDBC Schema 客户端
     * @param metadataReader 原生 JDBC 表结构读取器
     * @return 可复用的同步 DDL 入口
     */
    public static SyncDdlOperator create(JdbcSchemaClient schemaClient, JdbcFormMetadataReader metadataReader) {
        return new SyncDdlOperator(schemaClient, metadataReader);
    }

    /**
     * 开始描述一张表的目标结构。真正读取元数据和执行 DDL 都在后续的 commit、plan 或 review 中发生。
     *
     * @param table 物理表名
     * @return 单次使用的同步结构构建器
     */
    public SyncCreateOrAlterTableBuilder createOrAlter(String table) {
        return new SyncCreateOrAlterTableBuilder(jdbcSchemaClient, jdbcMetadataReader, table);
    }
}
