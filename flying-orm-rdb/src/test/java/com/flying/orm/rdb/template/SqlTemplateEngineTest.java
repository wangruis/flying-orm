package com.flying.orm.rdb.template;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 验证模板正文只在服务端注册，值和动态标识符分别走参数绑定与方言白名单。 */
class SqlTemplateEngineTest {

    @Test
    void rendersRegisteredTemplateWithoutTouchingQuotedTextOrPostgresqlCast() {
        SqlTemplateRegistry registry = SqlTemplateRegistry.builder()
                .register(SqlTemplate.query(
                        "active-documents",
                        "select * from ${table} where tenant_id = :tenant and state = :state "
                                + "and note = ':ignored' and embedding::text <> ''",
                        Set.of("table")))
                .build();
        SqlTemplateEngine engine = SqlTemplateEngine.create(registry,
                                                            RdbDialect.postgresql(),
                                                            ValueCodecRegistry.standard());

        SqlRequest request = engine.render("active-documents",
                                           Map.of("tenant", "t-1", "state", State.ACTIVE),
                                           Map.of("table", "documents"));

        assertEquals("select * from \"documents\" where tenant_id = $1 and state = $2 "
                             + "and note = ':ignored' and embedding::text <> ''",
                     request.sql());
        assertEquals(java.util.List.of("t-1", "ACTIVE"), request.parameters());
    }

    @Test
    void rejectsUnregisteredSqlMissingParametersAndUnsafeIdentifiers() {
        SqlTemplateEngine engine = SqlTemplateEngine.create(
                SqlTemplateRegistry.builder()
                                   .register(SqlTemplate.query("by-id",
                                                               "select * from ${table} where id = :id",
                                                               Set.of("table")))
                                   .build(),
                RdbDialect.postgresql(),
                ValueCodecRegistry.standard());

        String unregistered = "select * from users -- must-not-leak";
        IllegalArgumentException unregisteredError = assertThrows(IllegalArgumentException.class,
                () -> engine.render(unregistered, Map.of(), Map.of()));
        assertFalse(unregisteredError.getMessage().contains(unregistered));
        assertThrows(IllegalArgumentException.class,
                     () -> engine.render("by-id", Map.of(), Map.of("table", "users")));
        assertThrows(IllegalArgumentException.class,
                     () -> engine.render("by-id", Map.of("id", 1), Map.of("table", "users; drop table users")));
        assertThrows(IllegalArgumentException.class,
                     () -> SqlTemplate.query("multi", "select 1; delete from users", Set.of()));

        Map<String, Object> nullable = new LinkedHashMap<>();
        nullable.put("id", null);
        java.util.List<Object> parameters = engine.render("by-id",
                                                          nullable,
                                                          Map.of("table", "users"))
                                                 .parameters();
        assertEquals(1, parameters.size());
        assertNull(parameters.getFirst());
    }

    /** SQL Server 模板在进入执行器前就要生成驱动使用的 @Pn 参数名。 */
    @Test
    void rendersSqlServerNamedParametersWithDriverMarkers() {
        SqlTemplateEngine engine = SqlTemplateEngine.create(
                SqlTemplateRegistry.builder()
                                   .register(SqlTemplate.query("update-name",
                                                               "select name from users where name = :name and id = :id",
                                                               Set.of()))
                                   .build(),
                RdbDialect.sqlServer(),
                ValueCodecRegistry.standard());

        SqlRequest request = engine.render("update-name",
                                           Map.of("name", "Alice", "id", 1L),
                                           Map.of());

        assertEquals("select name from users where name = @P0 and id = @P1", request.sql());
        assertEquals(java.util.List.of("Alice", 1L), request.parameters());
    }

    /** 查询模板不能靠工厂名字假装只读，注册阶段就要挡住明显的写入和 DDL。 */
    @Test
    void rejectsWriteStatementsRegisteredAsQueries() {
        assertThrows(IllegalArgumentException.class,
                     () -> SqlTemplate.query("update-user",
                                             "update users set enabled = :enabled where id = :id",
                                             Set.of()));
        assertThrows(IllegalArgumentException.class,
                     () -> SqlTemplate.query("delete-in-cte",
                                             "with removed as (delete from users returning id) select * from removed",
                                             Set.of()));
        assertThrows(IllegalArgumentException.class,
                     () -> SqlTemplate.query("drop-table", "drop table users", Set.of()));
    }

    /** 依赖方言模式的反斜杠引号不能让真实写关键字落入校验器误判的字符串区间。 */
    @Test
    void rejectsWritesHiddenByBackslashQuotedValues() {
        assertThrows(IllegalArgumentException.class,
                     () -> createEngine(SqlTemplate.query(
                             "mysql-backslash-write",
                             "with x as (select 'a\\'b') delete from users "
                                     + "where id = 1 and 'c\\'d' is not null",
                             Set.of()), RdbDialect.mysql()));
        assertThrows(IllegalArgumentException.class,
                     () -> createEngine(SqlTemplate.query(
                             "postgresql-escape-write",
                             "with x as (select E'a\\'b') delete from users "
                                     + "where id = 1 and E'c\\'d' is not null",
                             Set.of()), RdbDialect.postgresql()));
        assertThrows(IllegalArgumentException.class,
                     () -> createEngine(SqlTemplate.query(
                             "mysql-double-quote-write",
                             "with x as (select \"a\\\"b\") delete from users "
                                     + "where id = 1 and \"c\\\"d\" is not null",
                             Set.of()), RdbDialect.mysql()));
    }

    /** MySQL 中双减号后没有空白时不是注释，后续写关键字仍必须参与只读校验。 */
    @Test
    void rejectsWritesAfterMySqlDoubleMinusExpression() {
        assertThrows(IllegalArgumentException.class,
                     () -> createEngine(SqlTemplate.query(
                             "mysql-double-minus-write",
                             "with x as (select 1--1) delete from users where id = 1",
                             Set.of()), RdbDialect.mysql()));
        assertThrows(IllegalArgumentException.class,
                     () -> createEngine(SqlTemplate.query(
                             "mysql-non-ascii-space-write",
                             "with x as (select 1)--\u2003delete from users where id = 1",
                             Set.of()), RdbDialect.mysql()));
    }

    /** MySQL 块注释不嵌套，不能用第二个注释起点把首个结束符之后的写语句藏起来。 */
    @Test
    void rejectsWritesHiddenByNestedBlockCommentAssumption() {
        assertThrows(IllegalArgumentException.class,
                     () -> createEngine(SqlTemplate.query(
                             "mysql-nested-comment-write",
                             "with x as (select 1 /* outer /* inner */) "
                                     + "delete from users where id = 1 -- */",
                             Set.of()), RdbDialect.mysql()));
    }

    /** PostgreSQL dollar quote 在 MySQL 中可以成为标识符，注册期不能借方言未知的语法隐藏写操作。 */
    @Test
    void rejectsMySqlWriteBetweenDollarIdentifiers() {
        assertThrows(IllegalArgumentException.class,
                     () -> createEngine(SqlTemplate.query(
                             "mysql-dollar-identifier-write",
                             "with $tag$ as (select 1) delete from users "
                                     + "where id in (select * from $tag$)",
                             Set.of()), RdbDialect.mysql()));
    }

    /** PostgreSQL 标识符内部的 dollar tag 不是字符串边界，不能借此隐藏写关键字。 */
    @Test
    void rejectsPostgresqlWriteBetweenDollarIdentifiers() {
        assertThrows(IllegalArgumentException.class,
                     () -> createEngine(SqlTemplate.query(
                             "postgresql-dollar-identifier-write",
                             "with x as (select 1 as foo$tag$) delete from users "
                                     + "where id = 1 returning id as end$tag$",
                             Set.of()), RdbDialect.postgresql()));
    }

    /** SQL Server 嵌套块注释内的模板符号必须保持不透明。 */
    @Test
    void keepsSqlServerNestedBlockCommentsOpaque() {
        SqlTemplateEngine engine = createEngine(SqlTemplate.query(
                "sqlserver-nested-comment",
                "select 1 /* outer /* inner */ :ignored */ where id = :id",
                Set.of()), RdbDialect.sqlServer());

        SqlRequest request = engine.render("sqlserver-nested-comment", Map.of("id", 7L), Map.of());

        assertEquals("select 1 /* outer /* inner */ :ignored */ where id = @P0", request.sql());
        assertEquals(java.util.List.of(7L), request.parameters());
    }

    /** MySQL 井号行注释中的参数和写关键字都只是注释文本。 */
    @Test
    void keepsMySqlHashCommentsOpaque() {
        SqlTemplateEngine engine = createEngine(SqlTemplate.query(
                "mysql-hash-comment",
                "select 1 # delete from audit_log :ignored\nfrom users where id = :id",
                Set.of()), RdbDialect.mysql());

        SqlRequest request = engine.render("mysql-hash-comment", Map.of("id", 7L), Map.of());

        assertEquals("select 1 # delete from audit_log :ignored\nfrom users where id = ?", request.sql());
        assertEquals(java.util.List.of(7L), request.parameters());
    }

    /** MySQL 双减号表达式和非嵌套块注释之后的参数仍应被正常绑定。 */
    @Test
    void rendersParametersAfterMySqlCommentBoundaries() {
        SqlTemplateEngine engine = SqlTemplateEngine.create(
                SqlTemplateRegistry.builder()
                                   .register(SqlTemplate.query(
                                           "mysql-comment-boundaries",
                                           "select 1--1, /* outer /* inner */ + :value",
                                           Set.of()))
                                   .build(),
                RdbDialect.mysql(),
                ValueCodecRegistry.standard());

        SqlRequest request = engine.render("mysql-comment-boundaries", Map.of("value", 7L), Map.of());

        assertEquals("select 1--1, /* outer /* inner */ + ?", request.sql());
        assertEquals(java.util.List.of(7L), request.parameters());
    }

    /** MySQL 可执行注释不是普通注释，注册查询不能借它隐藏写入关键字。 */
    @Test
    void rejectsExecutableCommentsInReadOnlyTemplates() {
        IllegalArgumentException mysql = assertThrows(
                IllegalArgumentException.class,
                () -> SqlTemplate.query(
                        "mysql-executable-comment",
                        "select 1 /*!50000 update users set enabled = 0 */",
                        Set.of()));
        IllegalArgumentException mariaDb = assertThrows(
                IllegalArgumentException.class,
                () -> SqlTemplate.query(
                        "mariadb-executable-comment",
                        "select 1 /*M!100100 update users set enabled = 0 */",
                        Set.of()));

        assertEquals("SQL query template must not contain executable comments", mysql.getMessage());
        assertEquals("SQL query template must not contain executable comments", mariaDb.getMessage());
    }

    /** 模板槽位校验只报告稳定分类，不能把任意长度的注册输入带入异常。 */
    @Test
    void doesNotEchoInvalidIdentifierSlots() {
        String secret = "credential-fragment";

        IllegalArgumentException unsafe = assertThrows(IllegalArgumentException.class,
                () -> SqlTemplate.query("safe-id", "select 1", Set.of("slot-" + secret)));
        IllegalArgumentException duplicate = assertThrows(IllegalArgumentException.class,
                () -> SqlTemplate.query("safe-id", "select 1", Set.of(secret, " " + secret + " ")));

        assertFalse(unsafe.getMessage().contains(secret));
        assertFalse(duplicate.getMessage().contains(secret));
    }

    /** 参数名来自调用边界时，占位符集合校验也只能返回稳定类别，不能回显任意长度的调用方输入。 */
    @Test
    void doesNotEchoUnexpectedCallerParameterNames() {
        SqlTemplateEngine engine = SqlTemplateEngine.create(
                SqlTemplateRegistry.builder()
                                   .register(SqlTemplate.query("by-id",
                                                               "select * from users where id = :id",
                                                               Set.of()))
                                   .build(),
                RdbDialect.h2(),
                ValueCodecRegistry.standard());
        String secret = "credential-fragment-must-not-leak";

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> engine.render("by-id", Map.of("id", 1, secret, "value"), Map.of()));

        assertFalse(error.getMessage().contains(secret));
    }

    /** 模板占位符校验只返回稳定分类，不能回显任意长度的可信配置内容。 */
    @Test
    void doesNotEchoTemplatePlaceholderNamesWhenRenderingFails() {
        String secret = "credentialFragment".repeat(512);
        SqlTemplateEngine identifierEngine = SqlTemplateEngine.create(
                SqlTemplateRegistry.builder()
                                   .register(SqlTemplate.query(
                                           "identifier-template",
                                           "select ${" + secret + "}",
                                           Set.of()))
                                   .build(),
                RdbDialect.h2(),
                ValueCodecRegistry.standard());
        SqlTemplateEngine valueEngine = SqlTemplateEngine.create(
                SqlTemplateRegistry.builder()
                                   .register(SqlTemplate.query(
                                           "value-template",
                                           "select :" + secret,
                                           Set.of()))
                                   .build(),
                RdbDialect.h2(),
                ValueCodecRegistry.standard());

        IllegalArgumentException missingIdentifier = assertThrows(
                IllegalArgumentException.class,
                () -> identifierEngine.render("identifier-template", Map.of(), Map.of()));
        IllegalArgumentException missingValue = assertThrows(
                IllegalArgumentException.class,
                () -> valueEngine.render("value-template", Map.of(), Map.of()));

        assertEquals("SQL template identifier slot is not registered", missingIdentifier.getMessage());
        assertEquals("SQL template value is missing", missingValue.getMessage());
        assertFalse(missingIdentifier.getMessage().contains(secret));
        assertFalse(missingValue.getMessage().contains(secret));
    }

    /** 注册表冲突只报告稳定类别，不能回显模板标识或服务端安全参数名。 */
    @Test
    void doesNotEchoRegistryConflictNames() {
        String secret = "credentialFragment";
        SqlTemplate template = SqlTemplate.query(secret, "select 1", Set.of(secret));

        IllegalArgumentException duplicateServerParameter = assertThrows(
                IllegalArgumentException.class,
                () -> SqlTemplateRegistry.builder().register(
                        template, Set.of(secret, " " + secret + " ")));
        IllegalArgumentException conflictingSlot = assertThrows(
                IllegalArgumentException.class,
                () -> SqlTemplateRegistry.builder().register(template, Set.of(secret)));
        IllegalArgumentException duplicateTemplate = assertThrows(
                IllegalArgumentException.class,
                () -> SqlTemplateRegistry.builder().register(template).register(template));

        assertFalse(duplicateServerParameter.getMessage().contains(secret));
        assertFalse(conflictingSlot.getMessage().contains(secret));
        assertFalse(duplicateTemplate.getMessage().contains(secret));
    }

    @Test
    void ignoresPlaceholdersInsideCommentsAndDatabaseSpecificQuotedText() {
        SqlTemplateEngine postgresql = SqlTemplateEngine.create(
                SqlTemplateRegistry.builder()
                                   .register(SqlTemplate.query(
                                           "postgresql-text",
                                           "select $$:ignored ${ignored}$$, $body$:also_ignored$body$ from ${table} "
                                                   + "/* :outer /* :inner */ */ where id = :id -- :line_ignored",
                                           Set.of("table")))
                                   .build(),
                RdbDialect.postgresql(),
                ValueCodecRegistry.standard());

        SqlRequest postgresqlRequest = postgresql.render("postgresql-text",
                                                         Map.of("id", 7L),
                                                         Map.of("table", "documents"));

        assertEquals("select $$:ignored ${ignored}$$, $body$:also_ignored$body$ from \"documents\" "
                             + "/* :outer /* :inner */ */ where id = $1 -- :line_ignored",
                     postgresqlRequest.sql());
        assertEquals(java.util.List.of(7L), postgresqlRequest.parameters());

        SqlTemplateEngine sqlServer = SqlTemplateEngine.create(
                SqlTemplateRegistry.builder()
                                   .register(SqlTemplate.query(
                                           "sqlserver-identifier",
                                           "select [value:name]]suffix] from ${table} where id = :id",
                                           Set.of("table")))
                                   .build(),
                RdbDialect.sqlServer(),
                ValueCodecRegistry.standard());

        SqlRequest sqlServerRequest = sqlServer.render("sqlserver-identifier",
                                                       Map.of("id", 8L),
                                                       Map.of("table", "documents"));

        assertEquals("select [value:name]]suffix] from [documents] where id = @P0",
                     sqlServerRequest.sql());
        assertEquals(java.util.List.of(8L), sqlServerRequest.parameters());
    }

    /** Oracle 替代引号内部允许直接出现单引号，其中的模板符号和写关键字仍只是文本。 */
    @Test
    void keepsOracleAlternativeQuotedTextOpaqueToTemplateScanning() {
        String literal = "q'[Mary's ? :ignored ${ignored} delete from audit_log]'";
        String nationalLiteral = "nq'<It's ? :ignored ${ignored} update audit_log>'";
        SqlTemplateEngine oracle = SqlTemplateEngine.create(
                SqlTemplateRegistry.builder()
                                   .register(SqlTemplate.query(
                                           "oracle-text",
                                           "select " + literal + " as message, " + nationalLiteral
                                                   + " as national_message from ${table} where id = :id",
                                           Set.of("table")))
                                   .build(),
                RdbDialect.oracle(),
                ValueCodecRegistry.standard());

        SqlRequest request = oracle.render("oracle-text",
                                           Map.of("id", 7L),
                                           Map.of("table", "documents"));

        assertEquals("select " + literal + " as message, " + nationalLiteral
                             + " as national_message from \"documents\" where id = ?",
                     request.sql());
        assertEquals(java.util.List.of(7L), request.parameters());
    }

    /** 注册时就把允许的名称收拾干净，避免配置带空格后拖到第一次查询才报错。 */
    @Test
    void normalizesRegisteredIdentifierAndServerParameterNames() {
        SqlTemplate template = SqlTemplate.query("tenant-table",
                                                 "select * from ${table} where tenant_id = :tenantId",
                                                 Set.of(" table "));
        SqlTemplateRegistry registry = SqlTemplateRegistry.builder()
                .register(template, Set.of(" tenantId "))
                .build();
        SqlTemplateEngine engine = SqlTemplateEngine.create(registry,
                                                            RdbDialect.mysql(),
                                                            ValueCodecRegistry.standard());

        assertEquals(Set.of("table"), template.identifierSlots());
        assertEquals(Set.of("tenantId"), registry.serverParameters("tenant-table"));
        assertEquals("select * from `users` where tenant_id = ?",
                     engine.render("tenant-table",
                                   Map.of("tenantId", "tenant-a"),
                                   Map.of("table", "users"))
                           .sql());
    }

    private enum State {
        ACTIVE
    }

    private static SqlTemplateEngine createEngine(SqlTemplate template, RdbDialect dialect) {
        return SqlTemplateEngine.create(SqlTemplateRegistry.builder().register(template).build(),
                                        dialect,
                                        ValueCodecRegistry.standard());
    }
}
