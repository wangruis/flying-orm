package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import io.r2dbc.h2.H2ConnectionConfiguration;
import io.r2dbc.h2.H2ConnectionFactory;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 用真实 H2 R2DBC 验证可以把已有表结构读回动态表单。
 *
 * @author wangr
 * @date 2026-07-28
 * @version v1.0
 */
class H2ReactiveFormMetadataReaderTest {

    /**
     * 反向读取要保留字段顺序、主键、类型长度、数字精度和列注释。
     */
    @Test
    void readsDynamicFormFromH2InformationSchema() {
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(new H2ConnectionFactory(
                H2ConnectionConfiguration.builder()
                                         .inMemory("metadata_reader")
                                         .property("DB_CLOSE_DELAY", "-1")
                                         .build()));
        ReactiveFormMetadataReader reader = H2ReactiveFormMetadataReader.create(executor);

        Mono<DynamicForm> scenario = executor.rowsUpdated(SqlRequest.nativeSql("""
                                                                      create table Users (
                                                                          id bigint primary key,
                                                                          name varchar(64),
                                                                          amount decimal(16,2)
                                                                      )
                                                                      """,
                                                                      List.of()))
                                                 .then(executor.rowsUpdated(SqlRequest.nativeSql(
                                                         "comment on column Users.name is 'Name'",
                                                         List.of())))
                                                 .then(reader.readForm("users", "PUBLIC", "USERS"));

        StepVerifier.create(scenario)
                    .assertNext(form -> {
                        assertEquals("users", form.id());
                        assertEquals("PUBLIC.USERS", form.table());
                        assertEquals(3, form.fields().size());

                        DynamicField id = form.field("ID");
                        assertTrue(id.primaryKey());
                        assertEquals("BIGINT", id.dataType());

                        DynamicField name = form.field("NAME");
                        assertEquals("VARCHAR", name.dataType());
                        assertEquals(64, name.length());
                        assertEquals("Name", name.comment());

                        DynamicField amount = form.field("AMOUNT");
                        assertEquals("DECIMAL", amount.dataType());
                        assertEquals(16, amount.precision());
                        assertEquals(2, amount.scale());
                    })
                    .verifyComplete();
    }

    /**
     * 同一列同时属于主键和外键时，列元数据仍只能返回一次，并且必须保留主键标记。
     */
    @Test
    void readsPrimaryKeyColumnOnceWhenItAlsoBelongsToForeignKey() {
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(new H2ConnectionFactory(
                H2ConnectionConfiguration.builder()
                                         .inMemory("metadata_reader_primary_foreign_key")
                                         .property("DB_CLOSE_DELAY", "-1")
                                         .build()));
        ReactiveFormMetadataReader reader = H2ReactiveFormMetadataReader.create(executor);

        Mono<DynamicForm> scenario = executor.rowsUpdated(SqlRequest.nativeSql(
                                                         "create table PARENTS (id bigint primary key)",
                                                         List.of()))
                                                 .then(executor.rowsUpdated(SqlRequest.nativeSql("""
                                                         create table CHILDREN (
                                                             id bigint primary key,
                                                             constraint fk_children_parent foreign key(id)
                                                                 references PARENTS(id)
                                                         )
                                                         """, List.of())))
                                                 .then(reader.readForm("children", "PUBLIC", "CHILDREN"));

        StepVerifier.create(scenario)
                    .assertNext(form -> {
                        assertEquals(1, form.fields().size());
                        assertTrue(form.field("ID").primaryKey());
                    })
                    .verifyComplete();
    }

    /**
     * 裸表名必须按当前 Schema 的可见对象解析，不能把其他 Schema 的同名表、索引和外键合并进来。
     */
    @Test
    void scopesUnqualifiedTableMetadataToCurrentSchema() {
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(new H2ConnectionFactory(
                H2ConnectionConfiguration.builder()
                                         .inMemory("metadata_reader_current_schema")
                                         .property("DB_CLOSE_DELAY", "-1")
                                         .build()));
        ReactiveFormMetadataReader reader = H2ReactiveFormMetadataReader.create(executor);

        Mono<TableMetadata> scenario = executor.rowsUpdated(SqlRequest.nativeSql(
                                                         "create schema ARCHIVE", List.of()))
                                                 .then(executor.rowsUpdated(SqlRequest.nativeSql(
                                                         "create table PUBLIC.ACCOUNTS (id bigint primary key)",
                                                         List.of())))
                                                 .then(executor.rowsUpdated(SqlRequest.nativeSql(
                                                         "create table ARCHIVE.PARENTS (id bigint primary key)",
                                                         List.of())))
                                                 .then(executor.rowsUpdated(SqlRequest.nativeSql("""
                                                         create table ARCHIVE.ACCOUNTS (
                                                             id bigint primary key,
                                                             archived_name varchar(64),
                                                             parent_id bigint,
                                                             constraint fk_archive_accounts_parent
                                                                 foreign key(parent_id)
                                                                 references ARCHIVE.PARENTS(id)
                                                         )
                                                         """, List.of())))
                                                 .then(executor.rowsUpdated(SqlRequest.nativeSql(
                                                         "create index idx_archive_accounts_name "
                                                                 + "on ARCHIVE.ACCOUNTS(archived_name)",
                                                         List.of())))
                                                 .then(reader.readTable("ACCOUNTS"));

        StepVerifier.create(scenario)
                    .assertNext(table -> {
                        assertEquals("ACCOUNTS", table.name());
                        assertEquals(1, table.columns().size());
                        assertTrue(table.findColumn("ARCHIVED_NAME").isEmpty());
                        assertTrue(table.indexes().isEmpty());
                        assertTrue(table.foreignKeys().isEmpty());
                    })
                    .verifyComplete();
    }

    /**
     * quoted 物理表名区分大小写；元数据读取不能把同一 Schema 下仅大小写不同的表合并。
     */
    @Test
    void preservesCaseDistinctQuotedTablesInTheSameSchema() {
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(new H2ConnectionFactory(
                H2ConnectionConfiguration.builder()
                                         .inMemory("metadata_reader_case_distinct_tables")
                                         .property("DB_CLOSE_DELAY", "-1")
                                         .build()));
        ReactiveFormMetadataReader reader = H2ReactiveFormMetadataReader.create(executor);

        Mono<TableMetadata> scenario = executor.rowsUpdated(SqlRequest.nativeSql(
                                                         "create table PUBLIC.\"ParentMixed\" (\"id\" bigint primary key)",
                                                         List.of()))
                                                 .then(executor.rowsUpdated(SqlRequest.nativeSql("""
                                                         create table PUBLIC.\"CaseTable\" (
                                                             \"mixed_id\" bigint primary key,
                                                             \"parent_id\" bigint,
                                                             constraint \"fk_mixed\" foreign key(\"parent_id\")
                                                                 references PUBLIC.\"ParentMixed\"(\"id\")
                                                         )
                                                         """, List.of())))
                                                 .then(executor.rowsUpdated(SqlRequest.nativeSql(
                                                         "create index \"idx_mixed\" "
                                                                 + "on PUBLIC.\"CaseTable\"(\"parent_id\")",
                                                         List.of())))
                                                 .then(executor.rowsUpdated(SqlRequest.nativeSql(
                                                         "create table PUBLIC.\"CASETABLE\" (\"upper_id\" bigint primary key)",
                                                         List.of())))
                                                 .then(reader.readTable("PUBLIC", "CaseTable"));

        StepVerifier.create(scenario)
                    .assertNext(table -> {
                        assertEquals("PUBLIC.CaseTable", table.name());
                        assertEquals(2, table.columns().size());
                        assertTrue(table.findColumn("mixed_id").isPresent());
                        assertTrue(table.findColumn("upper_id").isEmpty());
                        assertEquals(List.of("parent_id"), table.index("idx_mixed").columns());
                        assertEquals(List.of("parent_id"), table.foreignKey("fk_mixed").columns());
                    })
                    .verifyComplete();
    }

    /**
     * 读完整表元数据时要把普通索引和唯一索引一起带回来，后面做差异迁移才知道数据库里已经有什么。
     */
    @Test
    void readsTableMetadataWithIndexes() {
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(new H2ConnectionFactory(
                H2ConnectionConfiguration.builder()
                                         .inMemory("metadata_reader_indexes")
                                         .property("DB_CLOSE_DELAY", "-1")
                                         .build()));
        ReactiveFormMetadataReader reader = H2ReactiveFormMetadataReader.create(executor);

        Mono<TableMetadata> scenario = executor.rowsUpdated(SqlRequest.nativeSql("""
                                                                      create table Orders (
                                                                          id bigint primary key,
                                                                          user_id bigint,
                                                                          order_no varchar(32)
                                                                      )
                                                                      """,
                                                                      List.of()))
                                                 .then(executor.rowsUpdated(SqlRequest.nativeSql(
                                                         "create unique index uk_orders_order_no on Orders(order_no)",
                                                         List.of())))
                                                 .then(executor.rowsUpdated(SqlRequest.nativeSql(
                                                         "create index idx_orders_user_id on Orders(user_id)",
                                                         List.of())))
                                                 .then(reader.readTable("PUBLIC", "ORDERS"));

        StepVerifier.create(scenario)
                    .assertNext(table -> {
                        assertEquals("PUBLIC.ORDERS", table.name());
                        assertEquals(3, table.columns().size());
                        assertEquals(List.of("ORDER_NO"), table.index("UK_ORDERS_ORDER_NO").columns());
                        assertTrue(table.index("UK_ORDERS_ORDER_NO").unique());
                        assertEquals(List.of("USER_ID"), table.index("IDX_ORDERS_USER_ID").columns());
                    })
                    .verifyComplete();
    }

    @Test
    void readsTableMetadataWithForeignKeys() {
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(new H2ConnectionFactory(
                H2ConnectionConfiguration.builder()
                                         .inMemory("metadata_reader_foreign_keys")
                                         .property("DB_CLOSE_DELAY", "-1")
                                         .build()));
        ReactiveFormMetadataReader reader = H2ReactiveFormMetadataReader.create(executor);

        Mono<TableMetadata> scenario = executor.rowsUpdated(SqlRequest.nativeSql("""
                                                                      create table Users (
                                                                          id bigint primary key
                                                                      )
                                                                      """,
                                                                      List.of()))
                                                 .then(executor.rowsUpdated(SqlRequest.nativeSql("""
                                                         create table Orders (
                                                             id bigint primary key,
                                                             user_id bigint,
                                                             constraint fk_orders_user foreign key(user_id) references Users(id)
                                                         )
                                                         """,
                                                         List.of())))
                                                 .then(reader.readTable("PUBLIC", "ORDERS"));

        StepVerifier.create(scenario)
                    .assertNext(table -> {
                        assertEquals(1, table.foreignKeys().size());
                        assertEquals(List.of("USER_ID"), table.foreignKey("FK_ORDERS_USER").columns());
                        assertEquals("USERS", table.foreignKey("FK_ORDERS_USER").referenceTable());
                        assertEquals(List.of("ID"), table.foreignKey("FK_ORDERS_USER").referenceColumns());
                    })
                    .verifyComplete();
    }
}
