package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.util.Objects;

/**
 * 同步元数据读取门面。
 *
 * <p>V2 的正常入口接收 {@link SyncSqlExecutor}，底层直接使用 JDBC 查询数据库字典，不创建 Reactor 链，
 * 也不会等待 R2DBC。V2 不再提供响应式 reader 转同步 reader 的等待桥，调用方必须明确配置 JDBC。</p>
 *
 * <p>门面只保存不可变运行时，可以作为单例并发共享。是否缓存由装配层决定，读取器本身不会偷偷创建
 * 无上限缓存，也不会拥有或关闭上层的数据源。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public final class SyncFormMetadataReader {

    private final Runtime runtime;

    private SyncFormMetadataReader(Runtime runtime) {
        this.runtime = Objects.requireNonNull(runtime, "sync metadata runtime must not be null");
    }

    /**
     * 从统一同步执行器创建原生 JDBC reader。
     *
     * @param executor 已装配事务参与和执行保护的同步执行器
     * @param dialect 已经明确或自动识别出的数据库方言
     * @return 可并发共享的同步元数据 reader
     */
    public static SyncFormMetadataReader create(SyncSqlExecutor executor, RdbDialect dialect) {
        JdbcFormMetadataReader reader = JdbcFormMetadataReaders.create(executor, dialect);
        return new SyncFormMetadataReader(new JdbcRuntime(reader));
    }

    /** 复用装配层已经应用缓存、方言和执行保护的 JDBC reader，避免创建第二份元数据状态。 */
    public static SyncFormMetadataReader create(JdbcFormMetadataReader reader) {
        return new SyncFormMetadataReader(new JdbcRuntime(Objects.requireNonNull(
                reader, "JDBC form metadata reader must not be null")));
    }

    /** 按 table 或 schema.table 读取动态表单。 */
    public DynamicForm readForm(String formId, String table) {
        return runtime.readForm(formId, table);
    }

    /** 按明确 schema 和表名读取动态表单。 */
    public DynamicForm readForm(String formId, String schema, String table) {
        return runtime.readForm(formId, schema, table);
    }

    /** 按 table 或 schema.table 读取字段、索引和外键元数据。 */
    public TableMetadata readTable(String table) {
        return runtime.readTable(table);
    }

    /** 按明确 schema 和表名读取字段、索引和外键元数据。 */
    public TableMetadata readTable(String schema, String table) {
        return runtime.readTable(schema, table);
    }

    /** 内部运行时只描述四个读取动作，SQL 和结构转换仍由既有 reader 负责。 */
    private interface Runtime {
        DynamicForm readForm(String formId, String table);

        DynamicForm readForm(String formId, String schema, String table);

        TableMetadata readTable(String table);

        TableMetadata readTable(String schema, String table);
    }

    /** 原生 JDBC 路径只是直接转发，不加入等待、线程切换或第二套转换逻辑。 */
    private record JdbcRuntime(JdbcFormMetadataReader reader) implements Runtime {

        private JdbcRuntime {
            Objects.requireNonNull(reader, "JDBC form metadata reader must not be null");
        }

        @Override
        public DynamicForm readForm(String formId, String table) {
            return reader.readForm(formId, table);
        }

        @Override
        public DynamicForm readForm(String formId, String schema, String table) {
            return reader.readForm(formId, schema, table);
        }

        @Override
        public TableMetadata readTable(String table) {
            return reader.readTable(table);
        }

        @Override
        public TableMetadata readTable(String schema, String table) {
            return reader.readTable(schema, table);
        }
    }

}
