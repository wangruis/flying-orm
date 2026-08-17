package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证动态表单结构可以渲染为稳定的关系型数据库结构 SQL。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
class FormSchemaSqlRendererTest {

    /** 保护字段的建表计划必须保存物理列和物理唯一索引，不能把唯一约束错误建在随机密文列上。 */
    @Test
    void createsProtectedPhysicalTargetAndBlindIndexInMigrationPlan() {
        DynamicForm target = DynamicForm.builder("customer", "customer")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("contact", "VARCHAR").withUnique(true))
                                        .encrypted("contact", EncryptedFieldDefinition.builder()
                                                                                      .searchModes(
                                                                                              EncryptedSearchMode.EXACT,
                                                                                              EncryptedSearchMode.SUFFIX)
                                                                                      .suffixLengths(4)
                                                                                      .build())
                                        .build();

        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                                                        .createTablePlan(
                                                                target,
                                                                target.toTableMetadata().indexes());

        assertEquals("PROTECTED_BINARY", plan.target().field("contact").dataType());
        assertEquals(4, plan.target().fields().size());
        assertEquals(1, plan.targetIndexes().size());
        assertFalse(plan.targetIndexes().getFirst().columns().contains("contact"));
        assertTrue(plan.requests().stream().map(SqlRequest::sql)
                       .anyMatch(sql -> sql.contains("BYTEA")));
    }

    /** 保护表合并自动索引时不得静默吞掉同名但定义不同的显式索引。 */
    @Test
    void rejectsConflictingExplicitIndexWhenProtectedSchemaAddsAutomaticIndexes() {
        DynamicForm target = DynamicForm.builder("customer", "customer")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("email", "VARCHAR").withUnique(true))
                                        .addField(DynamicField.of("contact", "VARCHAR"))
                                        .encrypted("contact", EncryptedFieldDefinition.builder().build())
                                        .build();
        String automaticName = target.toTableMetadata().indexes().getFirst().name();
        IndexMetadata conflicting = IndexMetadata.builder(automaticName)
                                                 .addColumn("id")
                                                 .build();

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                                           .createTablePlan(target, List.of(conflicting)));

        assertEquals("duplicate protected schema index name", error.getMessage());
    }

    /** CONTAINS 的侧索引表、查询索引、唯一索引和级联外键必须与新业务表一起进入建表计划。 */
    @Test
    void createsContainsTokenTableInTheSameReviewedSchemaPlan() {
        DynamicForm target = DynamicForm.builder("customer", "customer")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("contact", "VARCHAR"))
                                        .encrypted("contact", EncryptedFieldDefinition.builder()
                                                                                      .searchModes(
                                                                                              EncryptedSearchMode.CONTAINS)
                                                                                      .build())
                                        .build();

        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                                                        .createTablePlan(target, List.of());
        String sql = String.join("; ", plan.sqlTexts());

        assertTrue(sql.contains("create table \"__fop_c_"));
        assertTrue(sql.contains("\"field_tag\" VARCHAR(30) not null"));
        assertTrue(sql.contains("\"token_hash\" BYTEA not null"));
        assertTrue(sql.contains("on delete cascade"));
        assertEquals(2L, plan.sqlTexts().stream().filter(value -> value.startsWith("create ")
                && value.contains("index")).count());
        ReviewedSchemaMigrationPlan reviewed = SchemaMigrationReviewer.create(
                FormSchemaSqlRenderer.create(RdbDialect.postgresql()))
                                                                        .review(
                                                                                plan.target().toTableMetadata(),
                                                                                plan,
                                                                                SchemaMigrationReviewPolicy.allowBlocking());
        assertTrue(reviewed.rollback().requests().stream()
                           .map(SqlRequest::sql)
                           .anyMatch(value -> value.startsWith("drop table ")
                                   && value.contains("__fop_c_")));
    }

    @Test
    void doesNotEchoUnsafeRuntimeDataType() {
        String dataType = "varchar(32); drop table secret -- must-not-leak";
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> SchemaDialect.standard().dataType(dataType));
        String table = "credentialFragment.schema.table";
        IllegalArgumentException tableError = assertThrows(
                IllegalArgumentException.class,
                () -> RdbDialect.sqlServer().schema().columnCommentSql(table, "name", "comment"));

        assertFalse(error.getMessage().contains(dataType));
        assertFalse(tableError.getMessage().contains(table));
    }

    /** DDL 会话保护留在 Schema 包内，但各数据库的 setup/cleanup 语义仍要稳定。 */
    @Test
    void builtInDialectsRenderLockTimeoutSetupAndCleanup() {
        assertEquals(List.of("set session lock_wait_timeout = 2"),
                     RdbDialect.mysql().schema().lockTimeoutGuard(Duration.ofMillis(1500)).setupSqlTexts());
        assertEquals(List.of("set lock_timeout = '1500ms'"),
                     RdbDialect.postgresql().schema().lockTimeoutGuard(Duration.ofMillis(1500)).setupSqlTexts());
        assertEquals(List.of("alter session set ddl_lock_timeout = 2"),
                     RdbDialect.oracle().schema().lockTimeoutGuard(Duration.ofMillis(1500)).setupSqlTexts());
        assertEquals(List.of("set lock_timeout 1500"),
                     RdbDialect.sqlServer().schema().lockTimeoutGuard(Duration.ofMillis(1500)).setupSqlTexts());
        assertThrows(UnsupportedOperationException.class,
                     () -> RdbDialect.h2().schema().lockTimeoutGuard(Duration.ofSeconds(1)));
    }

    /** 各数据库的会话变量都有明确数值范围，越界配置不能推迟到数据库执行时才失败。 */
    @Test
    void rejectsLockTimeoutsOutsideBuiltInDialectRanges() {
        assertEquals(List.of("set session lock_wait_timeout = 31536000"),
                     RdbDialect.mysql().schema()
                               .lockTimeoutGuard(Duration.ofSeconds(31_536_000L)).setupSqlTexts());
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.mysql().schema()
                                     .lockTimeoutGuard(Duration.ofSeconds(31_536_001L)));

        assertEquals(List.of("set lock_timeout = '2147483647ms'"),
                     RdbDialect.postgresql().schema()
                               .lockTimeoutGuard(Duration.ofMillis(Integer.MAX_VALUE)).setupSqlTexts());
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.postgresql().schema()
                                     .lockTimeoutGuard(Duration.ofMillis((long) Integer.MAX_VALUE + 1L)));

        assertEquals(List.of("alter session set ddl_lock_timeout = 1000000"),
                     RdbDialect.oracle().schema()
                               .lockTimeoutGuard(Duration.ofSeconds(1_000_000L)).setupSqlTexts());
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.oracle().schema()
                                     .lockTimeoutGuard(Duration.ofSeconds(1_000_001L)));

        assertEquals(List.of("set lock_timeout 2147483647"),
                     RdbDialect.sqlServer().schema()
                               .lockTimeoutGuard(Duration.ofMillis(Integer.MAX_VALUE)).setupSqlTexts());
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.sqlServer().schema()
                                     .lockTimeoutGuard(Duration.ofMillis((long) Integer.MAX_VALUE + 1L)));
    }

    /**
     * 默认结构渲染策略叫 standard，名字要清楚表达它只是基础写法。
     */
    @Test
    void exposesStandardSchemaDialectFactoryName() {
        Set<String> factories = Arrays.stream(SchemaDialect.class.getDeclaredMethods())
                                      .filter(method -> Modifier.isPublic(method.getModifiers()))
                                      .filter(method -> Modifier.isStatic(method.getModifiers()))
                                      .filter(method -> method.getParameterCount() == 0)
                                      .filter(method -> SchemaDialect.class.equals(method.getReturnType()))
                                      .map(Method::getName)
                                      .collect(Collectors.toUnmodifiableSet());

        assertEquals(Set.of("standard"), factories);
    }

    /**
     * 验证动态表单可以生成建表 SQL。
     */
    @Test
    void rendersCreateTableFromDynamicForm() {
        DynamicForm form = DynamicForm.builder("userForm", "Users")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("name", "VARCHAR").withNullable(false))
                                      .build();

        List<SqlRequest> requests = FormSchemaSqlRenderer.create(RdbDialect.h2()).createTable(form);

        assertEquals(1, requests.size());
        assertEquals("create table Users (id BIGINT primary key, name VARCHAR not null)", requests.get(0).sql());
        assertEquals(List.of(), requests.get(0).parameters());
    }

    /** 复合主键必须渲染成一个表级约束，不能给每一列分别声明 primary key。 */
    @Test
    void rendersCompositePrimaryKeyAsOneTableConstraint() {
        DynamicForm form = DynamicForm.builder("membership", "membership")
                                      .addField(DynamicField.primaryKey("tenant_id", "BIGINT"))
                                      .addField(DynamicField.primaryKey("user_id", "BIGINT"))
                                      .build();

        String sql = FormSchemaSqlRenderer.create(RdbDialect.postgresql()).createTable(form).getFirst().sql();

        assertEquals("create table \"membership\" (\"tenant_id\" BIGINT not null, "
                             + "\"user_id\" BIGINT not null, primary key (\"tenant_id\", \"user_id\"))",
                     sql);
    }

    /** 直接建表入口也必须兑现 DynamicField.unique，不能只在迁移计划入口创建唯一索引。 */
    @Test
    void directCreateTableIncludesGeneratedUniqueIndexes() {
        DynamicForm form = DynamicForm.builder("users", "users")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("email", "VARCHAR").withUnique(true))
                                      .build();

        List<String> sql = FormSchemaSqlRenderer.create(RdbDialect.postgresql()).createTable(form)
                                                .stream().map(SqlRequest::sql).toList();

        assertEquals(2, sql.size());
        assertTrue(sql.get(1).startsWith("create unique index "));
        assertTrue(sql.get(1).endsWith(" on \"users\" (\"email\")"));
    }

    /** 无长度参数语义的物理类型不能被静默拼成数据库不接受的伪类型。 */
    @Test
    void rejectsTypeArgumentsForUnparameterizedPhysicalTypes() {
        DynamicForm form = DynamicForm.builder("payloads", "payloads")
                                      .addField(DynamicField.of("body", "BINARY").withLength(128))
                                      .build();

        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> FormSchemaSqlRenderer.create(RdbDialect.postgresql()).createTable(form));

        assertEquals("data type does not accept length or precision arguments: BYTEA", failure.getMessage());
    }

    /** PostgreSQL 与 SQL Server 的整数类型没有长度参数，不能生成 INTEGER(10) 或 INT(10)。 */
    @Test
    void rejectsIntegerLengthThatBuiltInDialectsCannotExecute() {
        DynamicForm form = DynamicForm.builder("counters", "counters")
                                      .addField(DynamicField.of("value", "INTEGER").withLength(10))
                                      .build();

        IllegalArgumentException postgresql = assertThrows(
                IllegalArgumentException.class,
                () -> FormSchemaSqlRenderer.create(RdbDialect.postgresql()).createTable(form));
        IllegalArgumentException sqlServer = assertThrows(
                IllegalArgumentException.class,
                () -> FormSchemaSqlRenderer.create(RdbDialect.sqlServer()).createTable(form));

        assertEquals("data type does not accept length or precision arguments: INTEGER", postgresql.getMessage());
        assertEquals("data type does not accept length or precision arguments: INT", sqlServer.getMessage());
    }

    /** 多词固定类型和位置敏感的 interval 也不能被通用长度逻辑拼成非法 DDL。 */
    @Test
    void rejectsArgumentsForFixedMultiWordAndPositionSensitiveTypes() {
        DynamicForm postgresqlDouble = DynamicForm.builder("metrics", "metrics")
                                                  .addField(DynamicField.of("value", "DOUBLE PRECISION")
                                                                        .withLength(10))
                                                  .build();
        DynamicForm sqlServerDateTime = DynamicForm.builder("events", "events")
                                                   .addField(DynamicField.of("created_at", "SMALLDATETIME")
                                                                         .withLength(3))
                                                   .build();
        DynamicForm oracleInterval = DynamicForm.builder("events", "events")
                                                .addField(DynamicField.of("retention", "INTERVAL YEAR TO MONTH")
                                                                      .withLength(2))
                                                .build();

        assertEquals("data type does not accept length or precision arguments: DOUBLE PRECISION",
                     assertThrows(IllegalArgumentException.class,
                                  () -> FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                                                             .createTable(postgresqlDouble)).getMessage());
        assertEquals("data type does not accept length or precision arguments: SMALLDATETIME",
                     assertThrows(IllegalArgumentException.class,
                                  () -> FormSchemaSqlRenderer.create(RdbDialect.sqlServer())
                                                             .createTable(sqlServerDateTime)).getMessage());
        assertEquals("data type does not accept length or precision arguments: INTERVAL YEAR TO MONTH",
                     assertThrows(IllegalArgumentException.class,
                                  () -> FormSchemaSqlRenderer.create(RdbDialect.oracle())
                                                             .createTable(oracleInterval)).getMessage());
    }

    /** 生成列要按当前数据库真正支持的数值类型校验，不能只看模糊的 numeric 前缀。 */
    @Test
    void rejectsUnsupportedIdentityTypeAndInvalidMysqlIdentityLayout() {
        DynamicForm postgresDecimal = DynamicForm.builder("events", "events")
                                                 .addField(DynamicField.primaryKey("id", "DECIMAL")
                                                                       .withPrecision(20, 0)
                                                                       .withGeneration(ValueGeneration.identity()))
                                                 .build();
        DynamicForm mysqlMultiple = DynamicForm.builder("events", "events")
                                               .addField(DynamicField.primaryKey("tenant_id", "BIGINT")
                                                                     .withGeneration(ValueGeneration.identity()))
                                               .addField(DynamicField.primaryKey("event_id", "BIGINT")
                                                                     .withGeneration(ValueGeneration.identity()))
                                               .build();

        assertThrows(IllegalArgumentException.class,
                     () -> FormSchemaSqlRenderer.create(RdbDialect.postgresql()).createTable(postgresDecimal));
        IllegalArgumentException mysqlFailure = assertThrows(
                IllegalArgumentException.class,
                () -> FormSchemaSqlRenderer.create(RdbDialect.mysql()).createTable(mysqlMultiple));
        assertEquals("mysql table must not declare more than one identity column", mysqlFailure.getMessage());
    }

    /** identity 数量、MySQL 索引位置和 SQL Server scale 必须在生成不可执行 DDL 前失败。 */
    @Test
    void rejectsInvalidIdentityLayoutsForBuiltInDialects() {
        DynamicForm mysqlComposite = DynamicForm.builder("events", "events")
                                                 .addField(DynamicField.primaryKey("tenant_id", "BIGINT"))
                                                 .addField(DynamicField.primaryKey("event_id", "BIGINT")
                                                                       .withGeneration(ValueGeneration.identity()))
                                                 .build();
        DynamicForm oracleMultiple = DynamicForm.builder("events", "events")
                                                .addField(DynamicField.of("event_id", "BIGINT")
                                                                      .withGeneration(ValueGeneration.identity()))
                                                .addField(DynamicField.of("audit_id", "BIGINT")
                                                                      .withGeneration(ValueGeneration.identity()))
                                                .build();
        DynamicForm sqlServerDecimal = DynamicForm.builder("events", "events")
                                                  .addField(DynamicField.primaryKey("event_id", "DECIMAL")
                                                                        .withPrecision(20, 2)
                                                                        .withGeneration(ValueGeneration.identity()))
                                                  .build();

        assertEquals("mysql identity column must be the first column of an index",
                     assertThrows(IllegalArgumentException.class,
                                  () -> FormSchemaSqlRenderer.create(RdbDialect.mysql())
                                                             .createTable(mysqlComposite)).getMessage());
        assertEquals("oracle table must not declare more than one identity column",
                     assertThrows(IllegalArgumentException.class,
                                  () -> FormSchemaSqlRenderer.create(RdbDialect.oracle())
                                                             .createTable(oracleMultiple)).getMessage());
        assertEquals("sql server generated decimal data type must have scale zero: DECIMAL(20,2)",
                     assertThrows(IllegalArgumentException.class,
                                  () -> FormSchemaSqlRenderer.create(RdbDialect.sqlServer())
                                                             .createTable(sqlServerDecimal)).getMessage());
    }

    /** MySQL 标识列起点要进入表选项，无法逐列表达的步长不能被静默忽略。 */
    @Test
    void rendersMysqlIdentityStartAndRejectsUnsupportedIncrement() {
        DynamicForm startAt = DynamicForm.builder("events", "events")
                                         .addField(DynamicField.primaryKey("id", "BIGINT")
                                                               .withGeneration(ValueGeneration.identity(100, 1, 0)))
                                         .build();
        DynamicForm increment = DynamicForm.builder("events", "events")
                                           .addField(DynamicField.primaryKey("id", "BIGINT")
                                                                 .withGeneration(ValueGeneration.identity(1, 2, 0)))
                                           .build();

        String sql = FormSchemaSqlRenderer.create(RdbDialect.mysql()).createTable(startAt).getFirst().sql();

        assertTrue(sql.endsWith(" auto_increment = 100"));
        assertEquals("mysql identity increment must be one",
                     assertThrows(IllegalArgumentException.class,
                                  () -> FormSchemaSqlRenderer.create(RdbDialect.mysql())
                                                             .createTable(increment)).getMessage());
    }

    /** Oracle 不接受 CACHE 1，方言必须在生成不可执行 DDL 前稳定拒绝。 */
    @Test
    void rejectsOracleSequenceCacheOfOne() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.primaryKey("id", "BIGINT")
                                                            .withGeneration(ValueGeneration.sequence(
                                                                    "events_id_seq", 1, 1, 1)))
                                      .build();

        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> FormSchemaSqlRenderer.create(RdbDialect.oracle()).createTable(form));

        assertEquals("oracle sequence cache size must be zero or at least two", failure.getMessage());
    }

    /** 直接迁移不能把约束变化伪装成类型变化后返回成功。 */
    @Test
    void directMigrationRejectsColumnConstraintChanges() {
        DynamicForm source = DynamicForm.builder("users", "users")
                                        .addField(DynamicField.of("name", "VARCHAR"))
                                        .build();
        DynamicForm target = DynamicForm.builder("users", "users")
                                        .addField(DynamicField.of("name", "VARCHAR").withNullable(false))
                                        .build();

        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> FormSchemaSqlRenderer.create(RdbDialect.postgresql()).migrate(source.diffTo(target)));

        assertEquals("column constraint changes require a reviewed migration plan", failure.getMessage());
    }

    /** 直接迁移新增唯一字段时必须同时创建稳定唯一索引，不能只加列后静默丢失数据约束。 */
    @Test
    void directMigrationCreatesUniqueIndexForAddedUniqueField() {
        DynamicForm source = DynamicForm.builder("users", "users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .build();
        DynamicForm target = DynamicForm.builder("users", "users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("external_reference", "VARCHAR").withUnique(true))
                                        .build();

        List<String> sql = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                                                .migrate(source.diffTo(target))
                                                .stream()
                                                .map(SqlRequest::sql)
                                                .toList();

        assertEquals(List.of(
                "alter table \"users\" add column \"external_reference\" VARCHAR(255)",
                "create unique index \"uk_users_external_reference\" "
                        + "on \"users\" (\"external_reference\")"), sql);
    }

    /** 只有注释变化时不应额外重写字段类型。 */
    @Test
    void directMigrationRendersOnlySupportedCommentChange() {
        DynamicForm source = DynamicForm.builder("users", "users")
                                        .addField(DynamicField.of("name", "VARCHAR"))
                                        .build();
        DynamicForm target = DynamicForm.builder("users", "users")
                                        .addField(DynamicField.of("name", "VARCHAR").withComment("display name"))
                                        .build();

        List<SqlRequest> requests = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                                                         .migrate(source.diffTo(target));

        assertEquals(List.of("comment on column \"users\".\"name\" is 'display name'"),
                     requests.stream().map(SqlRequest::sql).toList());
    }

    /** Oracle 删除字段注释使用空字符串；PostgreSQL 仍使用 NULL，不能把两种方言混成一条语法。 */
    @Test
    void removesColumnCommentsWithDialectSpecificSyntax() {
        TableMetadata current = TableMetadata.builder("users")
                                             .addColumn(ColumnMetadata.of("name", "VARCHAR")
                                                                      .withComment("display name"))
                                             .build();
        DynamicForm target = DynamicForm.builder("users", "users")
                                        .addField(DynamicField.of("name", "VARCHAR"))
                                        .build();

        assertEquals(List.of("comment on column \"users\".\"name\" is ''"),
                     FormSchemaSqlRenderer.create(RdbDialect.oracle())
                                          .migrateSafelyPlan(current, target, List.of())
                                          .sqlTexts());
        assertEquals(List.of("comment on column \"users\".\"name\" is null"),
                     FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                                          .migrateSafelyPlan(current, target, List.of())
                                          .sqlTexts());
    }

    /** MySQL MODIFY COLUMN 必须重放完整目标定义，避免扩容时丢失 NOT NULL 与内联注释。 */
    @Test
    void mysqlTypeMigrationReplaysTheCompleteColumnDefinition() {
        DynamicForm source = DynamicForm.builder("users", "users")
                                        .addField(DynamicField.of("name", "VARCHAR")
                                                              .withLength(32)
                                                              .withNullable(false)
                                                              .withComment("display name"))
                                        .build();
        DynamicForm target = DynamicForm.builder("users", "users")
                                        .addField(DynamicField.of("name", "VARCHAR")
                                                              .withLength(128)
                                                              .withNullable(false)
                                                              .withComment("display name"))
                                        .build();
        TableMetadata current = TableMetadata.builder("users")
                                             .addColumn(ColumnMetadata.of("name", "VARCHAR")
                                                                      .withLength(32)
                                                                      .withNullable(false)
                                                                      .withComment("display name"))
                                             .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.mysql());

        assertEquals(List.of("alter table `users` modify column `name` VARCHAR(128) not null "
                                     + "comment 'display name'"),
                     renderer.migrate(source.diffTo(target)).stream().map(SqlRequest::sql).toList());
        assertEquals(List.of("alter table `users` modify column `name` VARCHAR(128) not null "
                                     + "comment 'display name'"),
                     renderer.migrateSafelyPlan(current, target, List.of()).sqlTexts());
    }

    @Test
    void nullableDifferenceRequiresReviewInsteadOfBeingSilentlyAccepted() {
        TableMetadata current = TableMetadata.builder("users")
                                             .addColumn(ColumnMetadata.of("name", "VARCHAR"))
                                             .build();
        DynamicForm target = DynamicForm.builder("users", "users")
                                        .addField(DynamicField.of("name", "VARCHAR").withNullable(false))
                                        .build();

        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                                                        .migrateSafelyPlan(current, target, List.of());

        assertTrue(plan.requiresManualReview());
        assertEquals(SkippedSchemaChange.Kind.CHANGE_COLUMN, plan.skippedChanges().getFirst().kind());
    }

    /** 逻辑类型名称只改变大小写时仍是同一列结构，不能生成无意义 ALTER 或虚假的人工审核项。 */
    @Test
    void ignoresCaseOnlyLogicalDataTypeDifferences() {
        TableMetadata current = TableMetadata.builder("users")
                                             .addColumn(ColumnMetadata.of("name", "VARCHAR"))
                                             .build();
        DynamicForm target = DynamicForm.builder("users", "users")
                                        .addField(DynamicField.of("name", "varchar"))
                                        .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());

        SchemaMigrationPlan safe = renderer.migrateSafelyPlan(current, target, List.of());
        SchemaMigrationPlan allowed = renderer.migrateSafelyPlan(
                current,
                target,
                List.of(),
                SchemaMigrationOptions.safe().allowColumnChange());
        ReviewedSchemaMigrationPlan review = SchemaMigrationReviewer.create(renderer)
                                                                     .review(current,
                                                                             safe,
                                                                             SchemaMigrationReviewPolicy
                                                                                     .allowBlocking());

        assertTrue(safe.requests().isEmpty());
        assertTrue(safe.skippedChanges().isEmpty());
        assertTrue(allowed.requests().isEmpty());
        assertTrue(allowed.skippedChanges().isEmpty());
        assertTrue(review.rollback().requests().isEmpty());
        assertTrue(review.rollback().gaps().isEmpty());
    }

    /** 内联类型参数与结构化长度、精度表示相同时，迁移和回滚审核都不能制造虚假变更。 */
    @Test
    void treatsInlineAndStructuredTypeArgumentsAsTheSameStorageShape() {
        TableMetadata current = TableMetadata.builder("users")
                                             .addColumn(ColumnMetadata.of("name", "VARCHAR").withLength(32))
                                             .addColumn(ColumnMetadata.of("amount", "DECIMAL")
                                                                      .withPrecision(18, 4))
                                             .build();
        DynamicForm target = DynamicForm.builder("users", "users")
                                        .addField(DynamicField.of("name", "VARCHAR(32)"))
                                        .addField(DynamicField.of("amount", "NUMERIC(18, 4)"))
                                        .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());

        SchemaMigrationPlan plan = renderer.migrateSafelyPlan(current, target, List.of());
        ReviewedSchemaMigrationPlan review = SchemaMigrationReviewer.create(renderer)
                                                                     .review(current,
                                                                             plan,
                                                                             SchemaMigrationReviewPolicy
                                                                                     .allowBlocking());

        assertTrue(plan.requests().isEmpty());
        assertTrue(plan.skippedChanges().isEmpty());
        assertTrue(review.rollback().requests().isEmpty());
        assertTrue(review.rollback().gaps().isEmpty());
    }

    /** 内联类型参数仍须参与安全扩容判定，不能因为表示方式不同而退化成人工审核。 */
    @Test
    void widensInlineTypeArgumentsThroughTheSafeMigrationPath() {
        TableMetadata current = TableMetadata.builder("users")
                                             .addColumn(ColumnMetadata.of("name", "VARCHAR").withLength(32))
                                             .addColumn(ColumnMetadata.of("amount", "DECIMAL")
                                                                      .withPrecision(18, 4))
                                             .build();
        DynamicForm target = DynamicForm.builder("users", "users")
                                        .addField(DynamicField.of("name", "VARCHAR(64)"))
                                        .addField(DynamicField.of("amount", "NUMERIC(20, 4)"))
                                        .build();

        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                                                        .migrateSafelyPlan(current, target, List.of());

        assertEquals(List.of("alter table \"users\" alter column \"name\" type VARCHAR(64)",
                             "alter table \"users\" alter column \"amount\" type NUMERIC(20, 4)"),
                     plan.sqlTexts());
        assertTrue(plan.skippedChanges().isEmpty());
    }

    /** 显式物理类型不能借逻辑映射伪装成安全扩容，例如 SQL Server 的 VARCHAR 与 NVARCHAR。 */
    @Test
    void keepsDifferentPhysicalTypesOutOfTheSafeWideningPath() {
        TableMetadata current = TableMetadata.builder("users")
                                             .addColumn(ColumnMetadata.of("name", "VARCHAR").withLength(32))
                                             .build();
        DynamicForm target = DynamicForm.builder("users", "users")
                                        .addField(DynamicField.of("name", "VARCHAR(64)"))
                                        .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.sqlServer());

        SchemaMigrationPlan safe = renderer.migrateSafelyPlan(current, target, List.of());
        SchemaMigrationPlan allowed = renderer.migrateSafelyPlan(
                current,
                target,
                List.of(),
                SchemaMigrationOptions.safe().allowColumnChange());

        assertTrue(safe.requests().isEmpty());
        assertEquals(1, safe.skippedChanges().size());
        assertEquals(List.of("alter table [users] alter column [name] VARCHAR(64) null"), allowed.sqlTexts());
    }

    /** 超出迁移比较数值范围的数据库专用类型参数必须保守进入审核，不能让比较器本身抛出异常。 */
    @Test
    void keepsOversizedInlineTypeArgumentsOnTheReviewPath() {
        TableMetadata current = TableMetadata.builder("users")
                                             .addColumn(ColumnMetadata.of("name", "VARCHAR").withLength(32))
                                             .build();
        DynamicForm target = DynamicForm.builder("users", "users")
                                        .addField(DynamicField.of("name", "VARCHAR(999999999999999999999999)"))
                                        .build();

        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                                                        .migrateSafelyPlan(current, target, List.of());

        assertTrue(plan.requests().isEmpty());
        assertEquals(1, plan.skippedChanges().size());
    }

    /**
     * 五种内置方言必须明确写出各自的 nullable 语法。放宽约束不会破坏已有数据，可以进入安全计划；
     * 收紧约束必须先由调用方显式允许字段变更，避免启动时突然因为历史 null 数据失败。
     */
    @Test
    void rendersNullableChangesForAllBuiltInDialects() {
        TableMetadata notNull = TableMetadata.builder("users")
                                             .addColumn(ColumnMetadata.of("name", "VARCHAR")
                                                                      .withLength(64)
                                                                      .withNullable(false))
                                             .build();
        DynamicForm nullable = DynamicForm.builder("users", "users")
                                          .addField(DynamicField.of("name", "VARCHAR").withLength(64))
                                          .build();
        Map<RdbDialect, String> relaxSql = Map.of(
                RdbDialect.h2(), "alter table users alter column name drop not null",
                RdbDialect.mysql(), "alter table `users` modify column `name` VARCHAR(64)",
                RdbDialect.postgresql(), "alter table \"users\" alter column \"name\" drop not null",
                RdbDialect.oracle(), "alter table \"users\" modify (\"name\" null)",
                RdbDialect.sqlServer(), "alter table [users] alter column [name] NVARCHAR(64) null");
        relaxSql.forEach((dialect, expected) -> assertEquals(
                List.of(expected),
                FormSchemaSqlRenderer.create(dialect)
                                     .migrateSafelyPlan(notNull, nullable, List.of())
                                     .sqlTexts()));

        TableMetadata currentlyNullable = TableMetadata.builder("users")
                                                        .addColumn(ColumnMetadata.of("name", "VARCHAR")
                                                                                 .withLength(64))
                                                        .build();
        DynamicForm required = DynamicForm.builder("users", "users")
                                          .addField(DynamicField.of("name", "VARCHAR")
                                                                .withLength(64)
                                                                .withNullable(false))
                                          .build();
        Map<RdbDialect, String> tightenSql = Map.of(
                RdbDialect.h2(), "alter table users alter column name set not null",
                RdbDialect.mysql(), "alter table `users` modify column `name` VARCHAR(64) not null",
                RdbDialect.postgresql(), "alter table \"users\" alter column \"name\" set not null",
                RdbDialect.oracle(), "alter table \"users\" modify (\"name\" not null)",
                RdbDialect.sqlServer(), "alter table [users] alter column [name] NVARCHAR(64) not null");
        tightenSql.forEach((dialect, expected) -> {
            FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(dialect);
            SchemaMigrationPlan safe = renderer.migrateSafelyPlan(currentlyNullable, required, List.of());
            SchemaMigrationPlan reviewed = renderer.migrateSafelyPlan(
                    currentlyNullable,
                    required,
                    List.of(),
                    SchemaMigrationOptions.safe().allowColumnChange());

            assertEquals(List.of(), safe.sqlTexts());
            assertEquals(SkippedSchemaChange.Kind.CHANGE_COLUMN, safe.skippedChanges().getFirst().kind());
            assertEquals(List.of(expected), reviewed.sqlTexts());
        });
    }

    @Test
    void rejectsUnsafeSchemaIdentifiers() {
        DynamicForm unsafeTable = DynamicForm.builder("unsafeForm", "Users; drop table Users")
                                            .addField(DynamicField.of("name", "VARCHAR"))
                                            .build();
        DynamicForm unsafeField = DynamicForm.builder("unsafeForm", "Users")
                                            .addField(DynamicField.of("name or 1=1", "VARCHAR"))
                                            .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.mysql());

        assertThrows(IllegalArgumentException.class, () -> renderer.createTable(unsafeTable));
        assertThrows(IllegalArgumentException.class, () -> renderer.createTable(unsafeField));
    }

    /**
     * 字段类型最后会直接写进 DDL，所以不能让分号、注释或引号混进来。
     * 常见的长度、精度、多词类型和数组类型仍然要能正常使用。
     */
    @Test
    void rejectsUnsafeDataTypesButKeepsUsefulTypeSyntax() {
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());
        DynamicForm unsafe = DynamicForm.builder("unsafeType", "unsafe_type")
                                        .addField(DynamicField.of("payload", "VARCHAR(32); drop table users"))
                                        .build();
        DynamicForm safe = DynamicForm.builder("safeType", "safe_type")
                                      .addField(DynamicField.of("name", "VARCHAR(32)"))
                                      .addField(DynamicField.of("amount", "DECIMAL(18, 4)"))
                                      .addField(DynamicField.of("meeting_at", "TIME WITH TIME ZONE"))
                                      .addField(DynamicField.of("tags", "VARCHAR[]"))
                                      .build();

        assertThrows(IllegalArgumentException.class, () -> renderer.createTable(unsafe));
        assertEquals("create table \"safe_type\" (\"name\" VARCHAR(32), \"amount\" DECIMAL(18, 4), "
                             + "\"meeting_at\" TIME WITH TIME ZONE, \"tags\" VARCHAR[])",
                     renderer.createTable(safe).getFirst().sql());
    }

    @Test
    void placesTypeArgumentsBeforeTimeZoneAndArraySuffixes() {
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());
        DynamicForm form = DynamicForm.builder("parameterizedTypes", "parameterized_types")
                                      .addField(DynamicField.of("created_at", "TIMESTAMP WITH TIME ZONE")
                                                            .withPrecision(6, null))
                                      .addField(DynamicField.of("local_time", "TIME(3) WITHOUT TIME ZONE"))
                                      .addField(DynamicField.of("tags", "VARCHAR[]")
                                                            .withLength(32))
                                      .build();

        assertEquals("create table \"parameterized_types\" (\"created_at\" TIMESTAMP(6) WITH TIME ZONE, "
                             + "\"local_time\" TIME(3) WITHOUT TIME ZONE, \"tags\" VARCHAR(32)[])",
                     renderer.createTable(form).getFirst().sql());
    }

    /** 数组参数能力由元素类型决定，不能生成 PostgreSQL 不接受的 TEXT(32)[] 等类型。 */
    @Test
    void rejectsArgumentsOnPostgresqlArrayElementTypesThatDoNotAcceptThem() {
        SchemaDialect postgresql = RdbDialect.postgresql().schema();

        assertEquals("VARCHAR(32)[]", postgresql.dataType("VARCHAR[]", 32, null, null));
        assertThrows(IllegalArgumentException.class,
                     () -> postgresql.dataType("TEXT[]", 32, null, null));
        assertThrows(IllegalArgumentException.class,
                     () -> postgresql.dataType("BYTEA[]", 32, null, null));
        assertThrows(IllegalArgumentException.class,
                     () -> postgresql.dataType("INTEGER[]", null, 3, null));
    }

    @Test
    void acceptsCommonVendorTypeModifiersWithoutOpeningDdlInjection() {
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(SchemaDialect.standard());
        DynamicForm form = DynamicForm.builder("advancedTypes", "advanced_types")
                                      .addField(DynamicField.of("counter", "INT UNSIGNED")
                                                            .withLength(11))
                                      .addField(DynamicField.of("local_time",
                                                                "TIMESTAMP WITH LOCAL TIME ZONE")
                                                            .withPrecision(6, null))
                                      .addField(DynamicField.of("active_for", "INTERVAL DAY TO SECOND"))
                                      .addField(DynamicField.of("billing_period", "INTERVAL YEAR TO MONTH"))
                                      .build();

        assertEquals("create table advanced_types (counter INT(11) UNSIGNED, "
                             + "local_time TIMESTAMP(6) WITH LOCAL TIME ZONE, "
                             + "active_for INTERVAL DAY TO SECOND, "
                             + "billing_period INTERVAL YEAR TO MONTH)",
                     renderer.createTable(form).getFirst().sql());
    }

    /**
     * 验证表单版本差异可以生成确定顺序的 alter SQL。
     */
    @Test
    void rendersAlterStatementsFromDynamicFormChangeSet() {
        DynamicForm source = DynamicForm.builder("userForm", "Users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("name", "VARCHAR"))
                                        .addField(DynamicField.of("age", "INTEGER"))
                                        .build();
        DynamicForm target = DynamicForm.builder("userForm", "Users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("name", "TEXT"))
                                        .addField(DynamicField.of("email", "VARCHAR"))
                                        .build();

        List<SqlRequest> requests = FormSchemaSqlRenderer.create(RdbDialect.h2()).migrate(source.diffTo(target));

        assertEquals(List.of("alter table Users add column email VARCHAR",
                             "alter table Users alter column name type TEXT",
                             "alter table Users drop column age"),
                     requests.stream().map(SqlRequest::sql).toList());
    }

    /** 直接变更集入口不能绕过受保护列的物理布局和历史明文迁移审核。 */
    @Test
    void rejectsDirectMigrationWhenProtectedFieldsRequireAReviewedPlan() {
        DynamicForm source = DynamicForm.builder("profiles", "profiles")
                                        .addField(DynamicField.of("contact", "VARCHAR"))
                                        .build();
        DynamicForm target = DynamicForm.builder("profiles", "profiles")
                                        .addField(DynamicField.of("contact", "VARCHAR"))
                                        .encrypted("contact", EncryptedFieldDefinition.builder().build())
                                        .build();

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> FormSchemaSqlRenderer.create(RdbDialect.h2()).migrate(source.diffTo(target)));

        assertEquals("protected fields require a reviewed schema migration plan", error.getMessage());
    }

    @Test
    void rendersSafeCreateOrAlterMigrationOnlyForMissingColumnsCommentsAndIndexes() {
        TableMetadata current = TableMetadata.builder("Users")
                                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                                             .addColumn(ColumnMetadata.of("name", "VARCHAR")
                                                                      .withLength(64))
                                             .addIndex(IndexMetadata.builder("idx_users_old")
                                                                    .addColumn("name")
                                                                    .build())
                                             .build();
        DynamicForm target = DynamicForm.builder("userForm", "Users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("name", "TEXT")
                                                              .withComment("Name"))
                                        .addField(DynamicField.of("email", "VARCHAR")
                                                              .withLength(128)
                                                              .withComment("Email"))
                                        .build();

        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                                                        .migrateSafelyPlan(current,
                                                                           target,
                                                                           List.of(IndexMetadata.builder("idx_users_email")
                                                                                                .addColumn("email")
                                                                                                .build()));

        assertEquals(List.of("comment on column \"Users\".\"name\" is 'Name'",
                             "alter table \"Users\" add column \"email\" VARCHAR(128)",
                             "comment on column \"Users\".\"email\" is 'Email'",
                             "create index \"idx_users_email\" on \"Users\" (\"email\")"),
                     plan.requests().stream().map(SqlRequest::sql).toList());
        assertTrue(plan.requiresManualReview());
        assertEquals(List.of(SkippedSchemaChange.Kind.CHANGE_COLUMN,
                             SkippedSchemaChange.Kind.DROP_INDEX),
                     plan.skippedChanges().stream().map(SkippedSchemaChange::kind).toList());
    }

    /**
     * 验证结构渲染器可以通过方言映射逻辑类型并处理标识符。
     */
    @Test
    void explicitMigrationOptionsAllowColumnChangeAndDropColumn() {
        TableMetadata current = TableMetadata.builder("Users")
                                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                                             .addColumn(ColumnMetadata.of("name", "VARCHAR")
                                                                      .withLength(64))
                                             .addColumn(ColumnMetadata.of("legacy", "VARCHAR"))
                                             .build();
        DynamicForm target = DynamicForm.builder("userForm", "Users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("name", "TEXT"))
                                        .build();

        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                                                        .migrateSafelyPlan(current,
                                                                           target,
                                                                           List.of(),
                                                                           SchemaMigrationOptions.safe()
                                                                                                 .allowColumnChange()
                                                                                                 .allowDropColumn());

        assertEquals(List.of("alter table \"Users\" alter column \"name\" type TEXT",
                             "alter table \"Users\" drop column \"legacy\""),
                     plan.sqlTexts());
        assertEquals(List.of(), plan.skippedChanges());
    }

    @Test
    void safeMigrationAllowsOnlyUnambiguousColumnWidening() {
        TableMetadata current = TableMetadata.builder("Users")
                                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                                             .addColumn(ColumnMetadata.of("name", "VARCHAR").withLength(64))
                                             .build();
        DynamicForm wider = DynamicForm.builder("userForm", "Users")
                                       .addField(DynamicField.primaryKey("id", "BIGINT"))
                                       .addField(DynamicField.of("name", "VARCHAR").withLength(128))
                                       .build();
        DynamicForm narrower = DynamicForm.builder("userForm", "Users")
                                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                                          .addField(DynamicField.of("name", "VARCHAR").withLength(32))
                                          .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());

        SchemaMigrationPlan widerPlan = renderer.migrateSafelyPlan(current, wider, List.of());
        SchemaMigrationPlan narrowerPlan = renderer.migrateSafelyPlan(current, narrower, List.of());

        assertEquals(1, widerPlan.executableSqlCount());
        assertEquals(0, widerPlan.skippedCount());
        assertEquals(0, narrowerPlan.executableSqlCount());
        assertEquals(1, narrowerPlan.skippedCount());
    }

    @Test
    void rendersIdentityAndNamedSequencesForTheMainDialects() {
        DynamicForm identity = DynamicForm.builder("device", "device")
                                          .addField(DynamicField.primaryKey("id", "BIGINT")
                                                                .withGeneration(ValueGeneration.identity()))
                                          .build();
        DynamicForm sequence = DynamicForm.builder("document", "document")
                                          .addField(DynamicField.primaryKey("id", "BIGINT")
                                                                .withGeneration(ValueGeneration.sequence("doc_id_seq")))
                                          .build();

        List<String> mysql = FormSchemaSqlRenderer.create(RdbDialect.mysql()).createTable(identity)
                                                  .stream().map(SqlRequest::sql).toList();
        List<String> h2 = FormSchemaSqlRenderer.create(RdbDialect.h2()).createTable(identity)
                                               .stream().map(SqlRequest::sql).toList();
        List<String> postgres = FormSchemaSqlRenderer.create(RdbDialect.postgresql()).createTable(sequence)
                                                     .stream().map(SqlRequest::sql).toList();

        assertTrue(mysql.getFirst().contains("auto_increment"));
        assertTrue(h2.getFirst().contains("generated by default as identity"));
        assertTrue(postgres.getFirst().startsWith("create sequence \"doc_id_seq\""));
        assertTrue(postgres.get(1).contains("nextval('\"doc_id_seq\"'::regclass)"));
    }

    @Test
    void explicitRenameHintProducesOneRenameInsteadOfAddAndDrop() {
        TableMetadata current = TableMetadata.builder("Users")
                                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                                             .addColumn(ColumnMetadata.of("display_name", "VARCHAR")
                                                                      .withLength(128))
                                             .addIndex(IndexMetadata.builder("idx_users_name")
                                                                    .addColumn("display_name")
                                                                    .build())
                                             .build();
        DynamicForm target = DynamicForm.builder("userForm", "Users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("name", "VARCHAR")
                                                              .withLength(128))
                                        .build();
        SchemaMigrationOptions options = SchemaMigrationOptions.safe()
                                                                 .renameColumn("display_name", "name");

        SchemaMigrationPlan postgresql = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                                                               .migrateSafelyPlan(current,
                                                                                  target,
                                                                                  List.of(IndexMetadata.builder("idx_users_name")
                                                                                                       .addColumn("name")
                                                                                                       .build()),
                                                                                  options);
        SchemaMigrationPlan sqlServer = FormSchemaSqlRenderer.create(RdbDialect.sqlServer())
                                                              .migrateSafelyPlan(current,
                                                                                 target,
                                                                                 List.of(IndexMetadata.builder("idx_users_name")
                                                                                                      .addColumn("name")
                                                                                                      .build()),
                                                                                 options);

        assertEquals(List.of("alter table \"Users\" rename column \"display_name\" to \"name\""),
                     postgresql.sqlTexts());
        assertEquals(List.of("exec sp_rename N'Users.display_name', N'name', N'COLUMN'"),
                     sqlServer.sqlTexts());
        assertEquals(List.of(), postgresql.skippedChanges());
    }

    /** 重命名声明及规划失败不能把调用方提供的无界列名复制进异常消息。 */
    @Test
    void doesNotEchoColumnRenameNames() {
        String secret = "release-review-secret-column-name";
        SchemaMigrationOptions duplicateSource = SchemaMigrationOptions.safe().renameColumn(secret, "name");
        IllegalArgumentException duplicateSourceError = assertThrows(
                IllegalArgumentException.class,
                () -> duplicateSource.renameColumn(secret, "other_name"));
        SchemaMigrationOptions duplicateTarget = SchemaMigrationOptions.safe().renameColumn("old_name", secret);
        IllegalArgumentException duplicateTargetError = assertThrows(
                IllegalArgumentException.class,
                () -> duplicateTarget.renameColumn("another_old_name", secret));

        TableMetadata current = TableMetadata.builder("Users")
                                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                                             .addColumn(ColumnMetadata.of("display_name", "VARCHAR"))
                                             .build();
        DynamicForm target = DynamicForm.builder("userForm", "Users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("name", "VARCHAR"))
                                        .build();
        IllegalArgumentException missingSource = assertThrows(
                IllegalArgumentException.class,
                () -> SchemaMigrationSupport.validateColumnRenames(
                        current, target, Map.of(secret, "name")));
        IllegalArgumentException missingTarget = assertThrows(
                IllegalArgumentException.class,
                () -> SchemaMigrationSupport.validateColumnRenames(
                        current, target, Map.of("display_name", secret)));

        assertFalse(duplicateSourceError.getMessage().contains(secret));
        assertFalse(duplicateTargetError.getMessage().contains(secret));
        assertFalse(missingSource.getMessage().contains(secret));
        assertFalse(missingTarget.getMessage().contains(secret));
    }

    @Test
    void explicitMigrationOptionsAllowDropAndRebuildIndexes() {
        TableMetadata current = TableMetadata.builder("Users")
                                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                                             .addColumn(ColumnMetadata.of("name", "VARCHAR"))
                                             .addColumn(ColumnMetadata.of("email", "VARCHAR"))
                                             .addIndex(IndexMetadata.builder("idx_users_old")
                                                                    .addColumn("name")
                                                                    .build())
                                             .addIndex(IndexMetadata.builder("idx_users_email")
                                                                    .addColumn("name")
                                                                    .build())
                                             .build();
        DynamicForm target = DynamicForm.builder("userForm", "Users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("name", "VARCHAR"))
                                        .addField(DynamicField.of("email", "VARCHAR"))
                                        .build();

        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.mysql())
                                                        .migrateSafelyPlan(current,
                                                                           target,
                                                                           List.of(IndexMetadata.builder("idx_users_email")
                                                                                                .addColumn("email")
                                                                                                .build()),
                                                                           SchemaMigrationOptions.safe()
                                                                                                 .allowDropIndex()
                                                                                                 .allowRebuildIndex());

        assertEquals(List.of("drop index `idx_users_email` on `Users`",
                             "create index `idx_users_email` on `Users` (`email`)",
                             "drop index `idx_users_old` on `Users`"),
                     plan.sqlTexts());
        assertEquals(List.of(), plan.skippedChanges());
    }

    @Test
    void rendersDropIndexSyntaxByDialect() {
        TableMetadata current = TableMetadata.builder("Users")
                                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                                             .addIndex(IndexMetadata.builder("idx_users_old")
                                                                    .addColumn("id")
                                                                    .build())
                                             .build();
        DynamicForm target = DynamicForm.builder("userForm", "Users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .build();
        SchemaMigrationOptions options = SchemaMigrationOptions.safe().allowDropIndex();

        assertEquals("drop index \"idx_users_old\"",
                     FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                                          .migrateSafelyPlan(current, target, List.of(), options)
                                          .sqlTexts()
                                          .getFirst());
        assertEquals("drop index `idx_users_old` on `Users`",
                     FormSchemaSqlRenderer.create(RdbDialect.mysql())
                                          .migrateSafelyPlan(current, target, List.of(), options)
                                          .sqlTexts()
                                          .getFirst());
        assertEquals("drop index \"idx_users_old\"",
                     FormSchemaSqlRenderer.create(RdbDialect.oracle())
                                          .migrateSafelyPlan(current, target, List.of(), options)
                                          .sqlTexts()
                                          .getFirst());
        assertEquals("drop index [idx_users_old] on [Users]",
                     FormSchemaSqlRenderer.create(RdbDialect.sqlServer())
                                          .migrateSafelyPlan(current, target, List.of(), options)
                                          .sqlTexts()
                                          .getFirst());
    }

    @Test
    void primaryKeyChangePlanShowsOldAndNewKeysWithManualSteps() {
        TableMetadata current = TableMetadata.builder("Users")
                                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                                             .addColumn(ColumnMetadata.of("code", "VARCHAR"))
                                             .build();
        DynamicForm target = DynamicForm.builder("userForm", "Users")
                                        .addField(DynamicField.of("id", "BIGINT"))
                                        .addField(DynamicField.primaryKey("code", "VARCHAR"))
                                        .build();

        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                                                        .migrateSafelyPlan(current,
                                                                           target,
                                                                           List.of(),
                                                                           SchemaMigrationOptions.safe()
                                                                                                 .allowPrimaryKeyChange());

        assertEquals(List.of(), plan.sqlTexts());
        assertEquals(1, plan.skippedCount());
        SkippedSchemaChange change = plan.skippedChanges().getFirst();
        assertEquals(SkippedSchemaChange.Kind.CHANGE_PRIMARY_KEY, change.kind());
        assertEquals("Users", change.name());
        assertEquals(Map.of("table", "Users",
                            "oldPrimaryKeys", List.of("id"),
                            "newPrimaryKeys", List.of("code"),
                            "requestedExecution", true,
                            "executionMode", "MANUAL_ONLY",
                            "requiresApprovalFingerprint", true),
                     change.details());
        assertEquals(List.of("review data that depends on the old primary key",
                             "write a checked migration script for the primary key change",
                             "run it from the upper layer in a controlled transaction or maintenance window",
                             "read table metadata again after the migration"),
                     change.suggestedSteps());
    }

    @Test
    void foreignKeyDifferencesStayInMigrationPlanForUpperLayerReview() {
        TableMetadata current = TableMetadata.builder("Orders")
                                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                                             .addColumn(ColumnMetadata.of("user_id", "BIGINT"))
                                             .addColumn(ColumnMetadata.of("org_id", "BIGINT"))
                                             .addForeignKey(ForeignKeyMetadata.builder("fk_orders_user")
                                                                              .addColumn("user_id")
                                                                              .referenceTable("users")
                                                                              .addReferenceColumn("id")
                                                                              .build())
                                             .addForeignKey(ForeignKeyMetadata.builder("fk_orders_old_org")
                                                                              .addColumn("org_id")
                                                                              .referenceTable("old_orgs")
                                                                              .addReferenceColumn("id")
                                                                              .build())
                                             .build();
        DynamicForm target = DynamicForm.builder("orderForm", "Orders")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("user_id", "BIGINT"))
                                        .addField(DynamicField.of("org_id", "BIGINT"))
                                        .build();

        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                                                        .migrateSafelyPlan(current,
                                                                           target,
                                                                           List.of(),
                                                                           List.of(ForeignKeyMetadata.builder("fk_orders_user")
                                                                                                     .addColumn("user_id")
                                                                                                     .referenceTable("members")
                                                                                                     .addReferenceColumn("id")
                                                                                                     .build(),
                                                                                   ForeignKeyMetadata.builder("fk_orders_org")
                                                                                                     .addColumn("org_id")
                                                                                                     .referenceTable("organizations")
                                                                                                     .addReferenceColumn("id")
                                                                                                     .build()),
                                                                           SchemaMigrationOptions.safe());

        assertEquals(List.of(), plan.sqlTexts());
        assertEquals(List.of(ForeignKeyMetadata.builder("fk_orders_user")
                                               .addColumn("user_id")
                                               .referenceTable("members")
                                               .addReferenceColumn("id")
                                               .build(),
                             ForeignKeyMetadata.builder("fk_orders_org")
                                               .addColumn("org_id")
                                               .referenceTable("organizations")
                                               .addReferenceColumn("id")
                                               .build()),
                     plan.targetForeignKeys());
        assertEquals(List.of(SkippedSchemaChange.Kind.CHANGE_FOREIGN_KEY,
                             SkippedSchemaChange.Kind.ADD_FOREIGN_KEY,
                             SkippedSchemaChange.Kind.DROP_FOREIGN_KEY),
                     plan.skippedChanges().stream().map(SkippedSchemaChange::kind).toList());
        assertEquals(Map.of("current", current.foreignKey("fk_orders_user"),
                            "target", plan.targetForeignKeys().getFirst()),
                     plan.skippedChanges().getFirst().details());
    }

    @Test
    void exposesReadableMigrationPlanSummaryForUpperLayers() {
        TableMetadata current = TableMetadata.builder("Users")
                                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                                             .addColumn(ColumnMetadata.of("legacy", "VARCHAR"))
                                             .build();
        DynamicForm target = DynamicForm.builder("userForm", "Users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("email", "VARCHAR")
                                                              .withLength(128))
                                        .build();

        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.mysql())
                                                        .migrateSafelyPlan(current, target, List.of());

        assertTrue(plan.hasExecutableSql());
        assertEquals(1, plan.skippedCount());
        assertEquals(List.of("alter table `Users` add column `email` VARCHAR(128)"), plan.sqlTexts());
        assertEquals(List.of("DROP_COLUMN legacy: SAFE mode does not drop existing columns"),
                     plan.skippedSummaries());
        assertEquals("DROP_COLUMN legacy: SAFE mode does not drop existing columns",
                     plan.skippedChanges().getFirst().summary());
    }

    @Test
    void rendersSchemaSqlWithDialectMappedTypesAndQuotedIdentifiers() {
        SchemaDialect dialect = SchemaDialect.builder()
                                             .quoteIdentifiers('"')
                                             .mapType("ID", "bigint")
                                             .mapType("TEXT_SHORT", "varchar(255)")
                                             .build();
        DynamicForm form = DynamicForm.builder("userForm", "Users")
                                      .addField(DynamicField.primaryKey("id", "ID"))
                                      .addField(DynamicField.of("name", "TEXT_SHORT"))
                                      .build();

        List<SqlRequest> requests = FormSchemaSqlRenderer.create(dialect).createTable(form);

        assertEquals("create table \"Users\" (\"id\" bigint primary key, \"name\" varchar(255))",
                     requests.get(0).sql());
    }

    @Test
    void rendersTypeArgumentsAndInlineCommentsForMysql() {
        DynamicForm form = DynamicForm.builder("userForm", "Users")
                                      .addField(DynamicField.primaryKey("id", "BIGINT")
                                                            .withComment("ID"))
                                      .addField(DynamicField.of("name", "VARCHAR")
                                                            .withLength(128)
                                                            .withComment("名称"))
                                      .addField(DynamicField.of("amount", "DECIMAL")
                                                            .withPrecision(32, 0))
                                      .build();

        List<SqlRequest> requests = FormSchemaSqlRenderer.create(RdbDialect.mysql()).createTable(form);

        assertEquals(1, requests.size());
        assertEquals("create table `Users` (`id` BIGINT primary key comment 'ID', "
                             + "`name` VARCHAR(128) comment '名称', `amount` DECIMAL(32,0))",
                     requests.get(0).sql());
    }

    @Test
    void rendersCommentOnColumnForPostgresql() {
        DynamicForm form = DynamicForm.builder("userForm", "Users")
                                      .addField(DynamicField.of("name", "VARCHAR")
                                                            .withLength(128)
                                                            .withComment("名称"))
                                      .build();

        List<SqlRequest> requests = FormSchemaSqlRenderer.create(RdbDialect.postgresql()).createTable(form);

        assertEquals(List.of("create table \"Users\" (\"name\" VARCHAR(128))",
                             "comment on column \"Users\".\"name\" is '名称'"),
                     requests.stream().map(SqlRequest::sql).toList());
    }

    /**
     * Oracle 首版方言要能把动态表单常用逻辑类型映射成更合适的列类型。
     */
    @Test
    void mapsCommonDynamicTypesForOracle() {
        List<SqlRequest> requests = FormSchemaSqlRenderer.create(RdbDialect.oracle())
                                                         .createTable(commonTypeForm());

        assertEquals("create table \"Dynamic_Form\" (\"id\" NUMBER(19) primary key, \"name\" VARCHAR2(255), "
                             + "\"enabled\" NUMBER(1), \"amount\" NUMBER(38,10), \"payload\" BLOB, "
                             + "\"description\" CLOB, \"created_at\" TIMESTAMP)",
                     requests.get(0).sql());
    }

    /**
     * MySQL 方言也要把动态表单常用逻辑类型映射成清晰的列类型。
     */
    @Test
    void mapsCommonDynamicTypesForMysql() {
        List<SqlRequest> requests = FormSchemaSqlRenderer.create(RdbDialect.mysql())
                                                         .createTable(commonTypeForm());

        assertEquals("create table `Dynamic_Form` (`id` BIGINT primary key, `name` VARCHAR(255), "
                             + "`enabled` BOOLEAN, `amount` DECIMAL(38,10), `payload` LONGBLOB, "
                             + "`description` TEXT, `created_at` DATETIME)",
                     requests.get(0).sql());
    }

    /**
     * PostgreSQL 方言也要把动态表单常用逻辑类型映射成清晰的列类型。
     */
    @Test
    void mapsCommonDynamicTypesForPostgresql() {
        List<SqlRequest> requests = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                                                         .createTable(commonTypeForm());

        assertEquals("create table \"Dynamic_Form\" (\"id\" BIGINT primary key, \"name\" VARCHAR(255), "
                             + "\"enabled\" BOOLEAN, \"amount\" NUMERIC(38,10), \"payload\" BYTEA, "
                             + "\"description\" TEXT, \"created_at\" TIMESTAMP)",
                     requests.get(0).sql());
    }

    /**
     * SQL Server 首版方言要能把动态表单常用逻辑类型映射成更合适的列类型。
     */
    @Test
    void mapsCommonDynamicTypesForSqlServer() {
        List<SqlRequest> requests = FormSchemaSqlRenderer.create(RdbDialect.sqlServer())
                                                         .createTable(commonTypeForm());

        assertEquals("create table [Dynamic_Form] ([id] BIGINT not null primary key, [name] NVARCHAR(255) null, "
                             + "[enabled] BIT null, [amount] DECIMAL(38,10) null, [payload] VARBINARY(max) null, "
                             + "[description] NVARCHAR(max) null, [created_at] DATETIME2 null)",
                     requests.get(0).sql());
    }

    @Test
    void rendersSqlServerExtendedPropertyColumnComment() {
        DynamicForm form = DynamicForm.builder("userForm", "dbo.Users")
                                      .addField(DynamicField.of("name", "VARCHAR")
                                                            .withLength(64)
                                                            .withComment("Name"))
                                      .build();

        List<SqlRequest> requests = FormSchemaSqlRenderer.create(RdbDialect.sqlServer()).createTable(form);

        assertEquals(List.of("create table [dbo].[Users] ([name] NVARCHAR(64) null)",
                             "exec sp_addextendedproperty @name = N'MS_Description', @value = N'Name', "
                                     + "@level0type = N'SCHEMA', @level0name = N'dbo', "
                                     + "@level1type = N'TABLE', @level1name = N'Users', "
                                     + "@level2type = N'COLUMN', @level2name = N'name'"),
                      requests.stream().map(SqlRequest::sql).toList());
    }

    /** SQL Server 注释必须拿到可信 schema，不能把未限定表静默猜成 dbo。 */
    @Test
    void requiresQualifiedSqlServerTableForColumnComments() {
        SchemaDialect sqlServer = RdbDialect.sqlServer().schema();

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> sqlServer.columnCommentSql("Users", "name", "Name"));

        assertEquals("SQL Server column comments require a schema-qualified table", error.getMessage());
        assertTrue(sqlServer.columnCommentSql("sales.Users", "name", "Name")
                            .orElseThrow()
                            .contains("@level0name = N'sales'"));
    }

    /** SQL Server 已存在的扩展属性必须更新或删除，不能重复调用 add 后在真实库失败。 */
    @Test
    void updatesAndRemovesSqlServerColumnComments() {
        TableMetadata current = TableMetadata.builder("dbo.Users")
                                             .addColumn(ColumnMetadata.of("name", "VARCHAR")
                                                                      .withLength(64)
                                                                      .withComment("Old name"))
                                             .build();
        DynamicForm updated = DynamicForm.builder("users", "dbo.Users")
                                         .addField(DynamicField.of("name", "VARCHAR")
                                                               .withLength(64)
                                                               .withComment("New name"))
                                         .build();
        DynamicForm removed = DynamicForm.builder("users", "dbo.Users")
                                         .addField(DynamicField.of("name", "VARCHAR").withLength(64))
                                         .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.sqlServer());

        List<String> updateSql = renderer.migrateSafelyPlan(current, updated, List.of()).sqlTexts();
        List<String> removeSql = renderer.migrateSafelyPlan(current, removed, List.of()).sqlTexts();

        assertEquals(1, updateSql.size());
        assertTrue(updateSql.getFirst().startsWith("exec sp_updateextendedproperty "));
        assertTrue(updateSql.getFirst().contains("@value = N'New name'"));
        assertEquals(1, removeSql.size());
        assertTrue(removeSql.getFirst().startsWith("exec sp_dropextendedproperty "));
    }

    /** PostgreSQL/Oracle 按名字删索引时必须保留表的 schema，避免误删同名索引或找不到目标。 */
    @Test
    void qualifiesNameOnlyDropIndexWithTableSchema() {
        assertEquals("drop index \"audit\".\"idx_users_old\"",
                     RdbDialect.postgresql().schema().dropIndexSql("audit.Users", "idx_users_old"));
        assertEquals("drop index \"audit\".\"idx_users_old\"",
                     RdbDialect.oracle().schema().dropIndexSql("audit.Users", "idx_users_old"));
        assertEquals("drop index \"audit\".\"idx_users_old\"",
                     RdbDialect.oracle().schema().dropIndexSql(" audit.Users ", " idx_users_old "));
    }

    /** Oracle 显式 schema 的索引创建与删除必须落在同一 schema，不能依赖当前登录用户。 */
    @Test
    void qualifiesOracleCreateIndexWithTableSchema() {
        IndexMetadata index = IndexMetadata.builder("idx_users_email").addColumn("email").build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.oracle());

        assertEquals("create index \"audit\".\"idx_users_email\" on \"audit\".\"Users\" (\"email\")",
                     renderer.createIndexes("audit.Users", List.of(index)).getFirst().sql());
        assertEquals("create index \"audit\".\"idx_users_email\" on \"audit\".\"Users\" (\"email\")",
                     renderer.createIndexes(" audit.Users ", List.of(index)).getFirst().sql());
        assertEquals("drop index \"audit\".\"idx_users_email\"",
                     RdbDialect.oracle().schema().dropIndexSql("audit.Users", index.name()));
    }

    /** 索引名和索引列只能是单段标识符，schema 只由目标表提供。 */
    @Test
    void rejectsQualifiedIndexNamesAndColumnsBeforeRendering() {
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());
        IndexMetadata qualifiedName = IndexMetadata.builder("audit.idx_users_email")
                                                   .addColumn("email")
                                                   .build();
        IndexMetadata qualifiedColumn = IndexMetadata.builder("idx_users_email")
                                                     .addColumn("users.email")
                                                     .build();

        assertThrows(IllegalArgumentException.class,
                     () -> renderer.createIndexes("audit.Users", List.of(qualifiedName)));
        assertThrows(IllegalArgumentException.class,
                     () -> renderer.createIndexes("audit.Users", List.of(qualifiedColumn)));
    }

    /**
     * Oracle 12.1 兼容后自动唯一索引名会缩短；既有较长自动名仍代表同一个唯一性约束，不能在安全同步中重复创建。
     */
    @Test
    void keepsOneLegacyAutomaticUniqueIndexWhenGeneratedNameIsShortened() {
        DynamicForm target = automaticUniqueTarget();
        IndexMetadata targetIndex = target.toTableMetadata().indexes().getFirst();
        IndexMetadata legacyIndex = IndexMetadata.builder("uk_customer_registry_external_reference_legacy")
                                                 .unique()
                                                 .addColumn("external_reference")
                                                 .build();
        TableMetadata current = currentWithIndexes(target, legacyIndex);

        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                                                        .migrateSafelyPlan(current,
                                                                           target,
                                                                           List.of(targetIndex));
        SchemaMigrationPlan destructivePlan = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                                                                   .migrateSafelyPlan(
                                                                           current,
                                                                           target,
                                                                           List.of(targetIndex),
                                                                           SchemaMigrationOptions.safe()
                                                                                                 .allowDropIndex());

        assertFalse(targetIndex.name().equals(legacyIndex.name()));
        assertTrue(plan.requests().isEmpty());
        assertTrue(plan.skippedChanges().isEmpty());
        assertTrue(destructivePlan.requests().isEmpty());
        assertTrue(destructivePlan.skippedChanges().isEmpty());
    }

    /** 多个旧索引形状相同而名称不同，无法证明来源时仍按严格名称规则处理，绝不任选一个消耗。 */
    @Test
    void doesNotGuessBetweenAmbiguousLegacyAutomaticUniqueIndexes() {
        DynamicForm target = automaticUniqueTarget();
        IndexMetadata targetIndex = target.toTableMetadata().indexes().getFirst();
        TableMetadata current = currentWithIndexes(
                target,
                IndexMetadata.builder("uk_legacy_first").unique().addColumn("external_reference").build(),
                IndexMetadata.builder("uk_legacy_second").unique().addColumn("external_reference").build());

        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                                                        .migrateSafelyPlan(current,
                                                                           target,
                                                                           List.of(targetIndex));

        assertEquals(1, plan.requests().size());
        assertTrue(plan.requests().getFirst().sql().contains("\"" + targetIndex.name() + "\""));
        assertEquals(2, plan.skippedChanges().size());
    }

    /** 调用方显式命名的唯一索引不享受自动名兼容，防止形状相同的旧索引改变其名称契约。 */
    @Test
    void keepsExplicitUniqueIndexNamesStrictWhenALegacyIndexHasTheSameColumn() {
        DynamicForm target = DynamicForm.builder("customerRegistry", "CustomerRegistry")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("external_reference", "VARCHAR"))
                                        .build();
        IndexMetadata explicit = IndexMetadata.builder("idx_customer_reference")
                                               .unique()
                                               .addColumn("external_reference")
                                               .build();
        TableMetadata current = currentWithIndexes(
                target,
                IndexMetadata.builder("uk_legacy_reference").unique().addColumn("external_reference").build());

        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                                                        .migrateSafelyPlan(current,
                                                                           target,
                                                                           List.of(explicit));

        assertEquals(1, plan.requests().size());
        assertTrue(plan.requests().getFirst().sql().contains("\"idx_customer_reference\""));
        assertEquals(1, plan.skippedChanges().size());
    }

    /** 自动索引兼容不猜字段改名；改名场景必须继续以显式 rename 和严格索引名称计划 DDL。 */
    @Test
    void doesNotReuseLegacyAutomaticUniqueIndexAcrossColumnRename() {
        DynamicForm target = automaticUniqueTarget();
        IndexMetadata targetIndex = target.toTableMetadata().indexes().getFirst();
        TableMetadata current = TableMetadata.builder(target.table())
                                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                                             .addColumn(ColumnMetadata.of("external_ref", "VARCHAR"))
                                             .addIndex(IndexMetadata.builder(
                                                             "uk_customer_registry_external_reference_legacy")
                                                                            .unique()
                                                                            .addColumn("external_ref")
                                                                            .build())
                                             .build();

        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                                                        .migrateSafelyPlan(
                                                                current,
                                                                target,
                                                                List.of(targetIndex),
                                                                SchemaMigrationOptions.safe().renameColumn(
                                                                        "external_ref",
                                                                        "external_reference"));

        assertEquals(2, plan.requests().size());
        assertTrue(plan.requests().getLast().sql().contains("\"" + targetIndex.name() + "\""));
        assertEquals(1, plan.skippedChanges().size());
        assertEquals(SkippedSchemaChange.Kind.DROP_INDEX, plan.skippedChanges().getFirst().kind());
    }

    private static DynamicForm commonTypeForm() {
        return DynamicForm.builder("dynamicForm", "Dynamic_Form")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .addField(DynamicField.of("enabled", "BOOLEAN"))
                          .addField(DynamicField.of("amount", "DECIMAL"))
                          .addField(DynamicField.of("payload", "BLOB"))
                          .addField(DynamicField.of("description", "TEXT"))
                          .addField(DynamicField.of("created_at", "TIMESTAMP"))
                          .build();
    }

    private static DynamicForm automaticUniqueTarget() {
        return DynamicForm.builder("customerRegistry", "CustomerRegistry")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("external_reference", "VARCHAR").withUnique(true))
                          .build();
    }

    private static TableMetadata currentWithIndexes(DynamicForm target, IndexMetadata... indexes) {
        TableMetadata.Builder builder = TableMetadata.builder(target.table());
        target.toTableMetadata().columns().forEach(builder::addColumn);
        for (IndexMetadata index : indexes) {
            builder.addIndex(index);
        }
        return builder.build();
    }
}
