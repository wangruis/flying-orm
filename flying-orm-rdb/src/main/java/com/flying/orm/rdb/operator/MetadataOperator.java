package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 读取数据库里已经存在的表结构，并把它转换成动态表单模型。
 *
 * <p>所有读取都是惰性的 Publisher，订阅前不获取连接。reader 启用 Caffeine 时，本对象也只操作同一份
 * 元数据缓存，不额外创建本地缓存；实例本身没有可变查询状态，可以并发共享。</p>
 *
 * @author wangr
 * @date 2026-07-28
 * @version v1.0
 */
public final class MetadataOperator {

    private final ReactiveFormMetadataReader reader;

    MetadataOperator(ReactiveFormMetadataReader reader) {
        this.reader = Objects.requireNonNull(reader, "reactive form metadata reader must not be null");
    }

    /** 每次订阅直读当前 Schema 可见的表和视图名称，不缓存、不加载逐表结构。 */
    public Flux<String> readTableNames() {
        return Flux.defer(reader::readTableNames);
    }

    /** 每次订阅直读指定 Schema 的表和视图名称；null 使用连接的当前 Schema。 */
    public Flux<String> readTableNames(String schema) {
        return Flux.defer(() -> reader.readTableNames(schema));
    }

    /** 每次订阅批量直读当前 Schema 的全部表结构。 */
    public Flux<RelationalTableDefinition> readTables() {
        return Flux.defer(reader::readTables);
    }

    /** 每次订阅批量直读指定 Schema 的全部表结构；null 使用当前 Schema。 */
    public Flux<RelationalTableDefinition> readTables(String schema) {
        return Flux.defer(() -> reader.readTables(schema));
    }

    /**
     * 按默认 schema 读取物理表并组装动态表单。
     *
     * @param formId 动态表单业务标识
     * @param table 物理表名
     * @return 订阅后读取到的动态表单
     */
    public Mono<DynamicForm> readForm(String formId, String table) {
        return reader.readForm(formId, table);
    }

    /**
     * 按明确 schema 读取物理表并组装动态表单。
     *
     * @param formId 动态表单业务标识
     * @param schema schema 名
     * @param table 物理表名
     * @return 订阅后读取到的动态表单
     */
    public Mono<DynamicForm> readForm(String formId, String schema, String table) {
        return reader.readForm(formId, schema, table);
    }

    /**
     * 按默认 schema 读取数据库原始表元数据。
     *
     * @param table 物理表名
     * @return 表、列、索引和外键元数据
     */
    public Mono<RelationalTableDefinition> readTable(String table) {
        return reader.readTable(table);
    }

    /**
     * 按明确 schema 读取数据库原始表元数据。
     *
     * @param schema schema 名
     * @param table 物理表名
     * @return 表、列、索引和外键元数据
     */
    public Mono<RelationalTableDefinition> readTable(String schema, String table) {
        return reader.readTable(schema, table);
    }

    /**
     * 清掉指定表的元数据缓存。reader 没有启用缓存时，这个方法什么也不做。
     *
     * <p>动态表单做完 DDL 后，上层最清楚哪张表变了，所以失效动作应该由上层明确触发。
     * 这样不会为了“猜测结构变化”去频繁查数据库，也不会把应用容器事件之类的上层机制塞进 ORM 内核。</p>
     *
     * @param table 物理表名，可以是 table 或 schema.table
     */
    public void invalidate(String table) {
        reader.invalidate(table);
    }

    /**
     * 清掉指定 schema 和表对应的缓存；没有启用缓存时是安全的空操作。
     *
     * @param schema schema 名
     * @param table 物理表名
     */
    public void invalidate(String schema, String table) {
        reader.invalidate(schema, table);
    }

    /**
     * 清空当前 reader 管理的全部元数据缓存；没有启用缓存时是安全的空操作。
     */
    public void invalidateAll() {
        reader.invalidateAll();
    }
}
