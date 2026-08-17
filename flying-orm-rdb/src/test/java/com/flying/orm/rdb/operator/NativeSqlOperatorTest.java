package com.flying.orm.rdb.operator;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证代码内原生 SQL 能直接执行，同时仍然坚持参数绑定、单语句和执行保护边界。
 *
 * @author wangr
 * @date 2026-08-02
 * @version v1.0
 */
class NativeSqlOperatorTest {

    /** 命名参数按 SQL 中的出现顺序展开，同一个名字出现多次也要重复绑定。 */
    @Test
    void compilesNamedParametersAndExecutesReactiveQuery() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        DatabaseOperator operator = operator(executor);

        StepVerifier.create(operator.unsafeNativeSql("select id from users where tenant_id = :tenant and owner_id = :owner "
                                                   + "or backup_owner_id = :owner")
                                    .bind("owner", 7L)
                                    .bind("tenant", "t-1")
                                    .query())
                    .expectNext(DynamicRow.copyOf(Map.of("id", 1L, "name", "Alice")))
                    .verifyComplete();

        assertEquals("select id from users where tenant_id = ? and owner_id = ? or backup_owner_id = ?",
                     executor.request.sql());
        assertEquals(List.of("t-1", 7L, 7L), executor.request.parameters());
    }

    /** null 是合法数据库参数，不能因为复制 Map 被提前拒绝。 */
    @Test
    void supportsNullAndTypedRowMapping() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        DatabaseOperator operator = operator(executor);

        StepVerifier.create(operator.unsafeNativeSql("select id, name from users where deleted_at is :deletedAt")
                                    .bind("deletedAt", null)
                                    .query(RowMapper.of(UserRow.class)))
                    .expectNext(new UserRow(1L, "Alice"))
                    .verifyComplete();

        assertEquals(1, executor.request.parameters().size());
        assertNull(executor.request.parameters().getFirst());
    }

    /** 查询保护选项必须继续传给统一执行器，不能被原生 SQL 入口绕过。 */
    @Test
    void passesExecutionOptionsAndExecutesWrite() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        DatabaseOperator operator = operator(executor);
        SqlExecutionOptions options = SqlExecutionOptions.maxRows(3).withTimeout(Duration.ofSeconds(1));

        StepVerifier.create(operator.unsafeNativeSql("select id from users where state = :state")
                                    .bind("state", "ACTIVE")
                                    .options(options)
                                    .query())
                    .expectNextCount(1)
                    .verifyComplete();
        assertEquals(options, executor.options);

        StepVerifier.create(operator.unsafeNativeSql("update users set state = :state where id = :id")
                                    .bindAll(Map.of("state", "DISABLED", "id", 1L))
                                    .execute())
                    .expectNext(1L)
                    .verifyComplete();
        assertEquals("update users set state = ? where id = ?", executor.request.sql());
        assertEquals(List.of("DISABLED", 1L), executor.request.parameters());
    }

    /** 缺少参数、多传参数、多语句都会在获取连接前失败。 */
    @Test
    void rejectsInvalidParameterSetsAndMultipleStatements() {
        DatabaseOperator operator = operator(new RecordingSqlExecutor());
        DatabaseOperator oracle = DatabaseOperator.create(new RecordingSqlExecutor(),
                                                           SqlRenderer.builder().addDefaultTerms().build(),
                                                           RdbDialect.oracle());
        DatabaseOperator sqlServer = DatabaseOperator.create(new RecordingSqlExecutor(),
                                                               SqlRenderer.builder().addDefaultTerms().build(),
                                                               RdbDialect.sqlServer());

        assertThrows(IllegalArgumentException.class,
                     () -> operator.unsafeNativeSql("select * from users where id = :id").toRequest());
        assertThrows(IllegalArgumentException.class,
                     () -> operator.unsafeNativeSql("select * from users").bind("unused", 1).toRequest());
        assertThrows(IllegalArgumentException.class,
                     () -> operator.unsafeNativeSql("select 1; delete from users"));
        assertThrows(IllegalArgumentException.class,
                     () -> operator.unsafeNativeSql("select 1; /* trailing comment */ delete from users"));
        assertThrows(IllegalArgumentException.class,
                     () -> oracle.unsafeNativeSql("begin null; end; drop table audit_log"));
        assertThrows(IllegalArgumentException.class,
                     () -> oracle.unsafeNativeSql("begin null; end; begin null; end;"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql("select 1\nselect 2"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql("update users set state = 'A'\ndelete from users"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql("set @a = 1\nset @b = 2"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql("grant select on users to app_user\nselect * from users"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql(
                             "grant execute on object::dbo.refresh_users to app_user\nselect 1"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql("deny select on users to app_user\ndelete from users"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql("use tempdb\nselect 1"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql("select 1\nuse tempdb"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql("declare @value int\nselect @value"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql("select 1\ncommit transaction"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql("select 1\nrollback transaction"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql("waitfor delay '00:00:01'\ndrop table audit_log"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql("select 1\nshutdown"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql("select 1\nbackup database app to disk = 'app.bak'"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql("select 1\nrestore database app from disk = 'app.bak'"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql("select 1\ndbcc checkdb"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql("select 1\nkill 52"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql("select 1\ncheckpoint"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql("select 1\nbulk insert users from 'users.csv'"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql("select 1\nsave transaction before_update"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql("select 1\nthrow"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql("select 1\nopen existing_cursor"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql("select 1\nclose existing_cursor"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql("select 1\ndeallocate existing_cursor"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql("select 1\nfetch next from existing_cursor"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql(
                             "select 1\nadd signature to object::dbo.refresh_users by certificate app_cert"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql(
                             "select 1\nadd sensitivity classification to dbo.users.email "
                                     + "with (label = 'Confidential')"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql(
                             "alter table users add nickname varchar(20)\ndrop table audit_log"));
    }

    /** 单语句词法边界不能误伤字符串、注释、CTE、集合查询和 INSERT SELECT。 */
    @Test
    void acceptsStatementKeywordsThatBelongToOneStatement() {
        DatabaseOperator operator = operator(new RecordingSqlExecutor());

        operator.unsafeNativeSql("select 'delete from users' as note -- update users\nfrom users");
        operator.unsafeNativeSql("with ids as (select id from users) select id from ids");
        operator.unsafeNativeSql("select id from users union select id from archived_users");
        operator.unsafeNativeSql("select id from users for update");
        operator.unsafeNativeSql("select ';' as separator");
        operator.unsafeNativeSql("select 1 -- ; remains comment text");
        operator.unsafeNativeSql("select 1;");
        operator.unsafeNativeSql("select 1; -- trailing comment");
        operator.unsafeNativeSql("select id from users order by id fetch first 10 rows only");
        operator.unsafeNativeSql("select id from users order by id offset 0 rows fetch next 10 rows only");
        operator.unsafeNativeSql("insert into archived_users(id) select id from users");
        operator.unsafeNativeSql("grant select, update on users to app_user");
        operator.unsafeNativeSql("grant execute on procedure refresh_users to app_user");
        operator.unsafeNativeSql("grant select on users to reporter with grant option");
        operator.unsafeNativeSql("deny select on users to app_user");
        operator.unsafeNativeSql("insert all into archived_users(id) values (1) "
                                         + "into archived_users(id) values (2) select 1 from dual");
        operator.unsafeNativeSql("insert first when 1 = 1 then into archived_users(id) values (1) "
                                         + "when 1 = 2 then into archived_users(id) values (2) select 1 from dual");
        operator.unsafeNativeSql("create table measurement_y2026 partition of measurement "
                                         + "for values from ('2026-01-01') to ('2027-01-01')");
        operator.unsafeNativeSql("alter table measurement attach partition measurement_y2026 "
                                         + "for values from ('2026-01-01') to ('2027-01-01')");
        operator.unsafeNativeSql("alter table users drop column obsolete_name");
        operator.unsafeNativeSql("create index users_idx on users(id) with (fillfactor = 70)");
        operator.unsafeNativeSql("select * from users with (updlock)");
        operator.unsafeNativeSql("select id from users for no key update");
        operator.unsafeNativeSql("alter table users add nickname varchar(20), drop column obsolete_name");
        operator.unsafeNativeSql("alter table users alter column nickname set not null");
        operator.unsafeNativeSql("alter table users alter column nickname set default 'anonymous'");
        operator.unsafeNativeSql("alter table users alter column nickname drop default");
        operator.unsafeNativeSql("alter table users alter column nickname drop not null");
        operator.unsafeNativeSql("backup database app to disk = 'app.bak'");
        operator.unsafeNativeSql("restore database app from disk = 'app.bak'");
        operator.unsafeNativeSql("dbcc checkdb");
        operator.unsafeNativeSql("checkpoint");
        operator.unsafeNativeSql("bulk insert users from 'users.csv'");
        operator.unsafeNativeSql("save transaction before_update");
        operator.unsafeNativeSql("create trigger users_audit after update on users "
                                         + "for each row execute function audit_users()");
        operator.unsafeNativeSql("merge into users u using staged_users s on (u.id = s.id) "
                                         + "when matched then update set u.name = s.name "
                                         + "when not matched then insert (id, name) values (s.id, s.name);");

        DatabaseOperator sqlServer = DatabaseOperator.create(new RecordingSqlExecutor(),
                                                               SqlRenderer.builder().addDefaultTerms().build(),
                                                               RdbDialect.sqlServer());
        sqlServer.unsafeNativeSql("alter table users add nickname varchar(20)");
        sqlServer.unsafeNativeSql("add signature to object::dbo.refresh_users by certificate app_cert");

        DatabaseOperator postgresql = DatabaseOperator.create(new RecordingSqlExecutor(),
                                                               SqlRenderer.builder().addDefaultTerms().build(),
                                                               RdbDialect.postgresql());
        postgresql.unsafeNativeSql("select 1 /* outer /* inner */ delete from users */");

    }

    /** 非 SQL Server 方言只做词法单语句检查，不能用通用关键字状态机误拒合法方言 SQL。 */
    @Test
    void acceptsDialectGrammarWithoutCrossDialectStatementGuessing() {
        DatabaseOperator oracle = DatabaseOperator.create(new RecordingSqlExecutor(),
                                                           SqlRenderer.builder().addDefaultTerms().build(),
                                                           RdbDialect.oracle());
        DatabaseOperator postgresql = DatabaseOperator.create(new RecordingSqlExecutor(),
                                                               SqlRenderer.builder().addDefaultTerms().build(),
                                                               RdbDialect.postgresql());

        assertAll(
                () -> oracle.unsafeNativeSql(
                        "select id from current_users minus select id from archived_users"),
                () -> oracle.unsafeNativeSql(
                        "merge into users d using staged_users s on (d.id = s.id) "
                                + "when matched then update set d.name = s.name "
                                + "delete where d.name is null "
                                + "when not matched then insert (id, name) values (s.id, s.name)"),
                () -> postgresql.unsafeNativeSql("select foo$tag$ from metrics"));
    }

    /** 同步门面使用原生 JDBC 同步执行链，编译规则与响应式入口保持一致。 */

    /** PostgreSQL 的问号运算符属于 SQL 正文，只有 :name 才会变成驱动参数标记。 */
    @Test
    void preservesPostgresqlQuestionOperatorAndCompilesNamedMarker() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        DatabaseOperator operator = DatabaseOperator.create(executor,
                                                            SqlRenderer.builder().addDefaultTerms().build(),
                                                            RdbDialect.postgresql());

        SqlRequest request = operator.unsafeNativeSql("select * from docs where payload ? 'enabled' and tenant_id = :tenant")
                                     .bind("tenant", "t-1")
                                     .toRequest();

        assertEquals("select * from docs where payload ? 'enabled' and tenant_id = $1", request.sql());
        assertEquals(List.of("t-1"), request.parameters());
        assertEquals(SqlBindMarkerStyle.NATIVE, request.bindMarkerStyle());
    }

    /** PostgreSQL 的井号是运算符而不是注释，后面的第二条语句不能被词法扫描器隐藏。 */
    @Test
    void rejectsPostgresqlStatementHiddenBehindHashOperator() {
        DatabaseOperator operator = DatabaseOperator.create(new RecordingSqlExecutor(),
                                                             SqlRenderer.builder().addDefaultTerms().build(),
                                                             RdbDialect.postgresql());

        assertThrows(IllegalArgumentException.class,
                     () -> operator.unsafeNativeSql("select 17 # 5; delete from users"));
    }

    /** SQL Server 的 UPDATE 赋值 SET 只能消费一次，换行后的会话 SET 是第二条语句。 */
    @Test
    void rejectsSqlServerSetAfterUpdateStatement() {
        DatabaseOperator operator = DatabaseOperator.create(new RecordingSqlExecutor(),
                                                             SqlRenderer.builder().addDefaultTerms().build(),
                                                             RdbDialect.sqlServer());

        assertThrows(IllegalArgumentException.class,
                     () -> operator.unsafeNativeSql(
                             "update users set active = 0 where id = 1\nset nocount on"));
    }

    /** 方言词法差异和未识别的首语句都必须失败闭合，不能把后续写语句交给驱动。 */
    @Test
    void rejectsDialectSpecificMultiStatementBypasses() {
        DatabaseOperator mysql = DatabaseOperator.create(new RecordingSqlExecutor(),
                                                           SqlRenderer.builder().addDefaultTerms().build(),
                                                           RdbDialect.mysql());
        DatabaseOperator sqlServer = DatabaseOperator.create(new RecordingSqlExecutor(),
                                                               SqlRenderer.builder().addDefaultTerms().build(),
                                                               RdbDialect.sqlServer());

        assertAll(
                () -> assertThrows(IllegalArgumentException.class,
                                   () -> mysql.unsafeNativeSql("select 1--1; delete from users")),
                () -> assertThrows(IllegalArgumentException.class,
                                   () -> sqlServer.unsafeNativeSql(
                                           "select 1\nwhile 1 = 1 continue")),
                () -> assertThrows(IllegalArgumentException.class,
                                   () -> sqlServer.unsafeNativeSql(
                                           "select 1\nloop_label:\ngoto loop_label")),
                () -> assertThrows(IllegalArgumentException.class,
                                   () -> sqlServer.unsafeNativeSql(
                                           "select 1\nreceive top (1) * from message_queue")),
                () -> assertThrows(IllegalArgumentException.class,
                                   () -> sqlServer.unsafeNativeSql("select 1\nrevert")),
                () -> assertThrows(IllegalArgumentException.class,
                                   () -> sqlServer.unsafeNativeSql("select 1\nsetuser 'mary'")),
                () -> assertThrows(IllegalArgumentException.class,
                                   () -> sqlServer.unsafeNativeSql(
                                           "select 1\nend conversation @dialog_handle")),
                () -> assertThrows(IllegalArgumentException.class,
                                   () -> sqlServer.unsafeNativeSql(
                                           "select 1\ndump database app to disk = 'app.bak'")),
                () -> assertThrows(IllegalArgumentException.class,
                                   () -> sqlServer.unsafeNativeSql(
                                           "select 1\nload database app from disk = 'app.bak'")),
                () -> assertThrows(IllegalArgumentException.class,
                                   () -> sqlServer.unsafeNativeSql(
                                           "select 1\nwritetext users.note @textptr 'changed'")),
                () -> assertThrows(IllegalArgumentException.class,
                                   () -> sqlServer.unsafeNativeSql(
                                           "enable trigger audit_trigger on users\ndelete from users")),
                () -> assertThrows(IllegalArgumentException.class,
                                   () -> sqlServer.unsafeNativeSql(
                                           "grant execute to app_user\ndrop table audit_log")),
                () -> assertThrows(IllegalArgumentException.class,
                                   () -> sqlServer.unsafeNativeSql(
                                           "select id from users for json path\n"
                                                   + "update statistics dbo.Users")),
                () -> assertThrows(IllegalArgumentException.class,
                                   () -> sqlServer.unsafeNativeSql("-- comment only")));
    }

    /** 受支持方言的合法复合 DDL 必须保留，安全边界不能退化成误伤常用单语句。 */
    @Test
    void acceptsDialectSpecificSingleStatementDdl() {
        DatabaseOperator mysql = DatabaseOperator.create(new RecordingSqlExecutor(),
                                                           SqlRenderer.builder().addDefaultTerms().build(),
                                                           RdbDialect.mysql());
        DatabaseOperator postgresql = DatabaseOperator.create(new RecordingSqlExecutor(),
                                                                SqlRenderer.builder().addDefaultTerms().build(),
                                                                RdbDialect.postgresql());
        DatabaseOperator oracle = DatabaseOperator.create(new RecordingSqlExecutor(),
                                                            SqlRenderer.builder().addDefaultTerms().build(),
                                                            RdbDialect.oracle());
        DatabaseOperator sqlServer = DatabaseOperator.create(new RecordingSqlExecutor(),
                                                               SqlRenderer.builder().addDefaultTerms().build(),
                                                               RdbDialect.sqlServer());

        mysql.unsafeNativeSql("create table archived_users select * from users");
        mysql.unsafeNativeSql("create table child (id int, parent_id int, constraint fk_parent "
                                      + "foreign key (parent_id) references parent (id) on delete set null)");
        mysql.unsafeNativeSql("alter table child add constraint fk_parent foreign key (parent_id) "
                                      + "references parent (id) on delete set null");
        mysql.unsafeNativeSql("create trigger users_bi before insert on users for each row "
                                      + "set new.created_at = current_timestamp");
        mysql.unsafeNativeSql("optimize table users");
        postgresql.unsafeNativeSql("alter table users set (fillfactor = 70)");
        postgresql.unsafeNativeSql("alter table users set schema archive");
        postgresql.unsafeNativeSql("vacuum analyze users");
        oracle.unsafeNativeSql("rename users to archived_users");
        oracle.unsafeNativeSql("begin dbms_session.sleep(10); end;");
        sqlServer.unsafeNativeSql("enable trigger audit_trigger on users");
        sqlServer.unsafeNativeSql("disable trigger audit_trigger on users");
        sqlServer.unsafeNativeSql("drop table if exists \"FLYING_ORM_SQLSERVER\"");
        sqlServer.unsafeNativeSql("setuser 'mary'");
        sqlServer.unsafeNativeSql("end conversation @dialog_handle");
    }

    /** 复合单语句中的关键字仍属于当前 DDL/DML，不能因为安全修复而误判成第二条语句。 */
    @Test
    void acceptsSupportedCompoundSingleStatements() {
        DatabaseOperator mysql = DatabaseOperator.create(new RecordingSqlExecutor(),
                                                           SqlRenderer.builder().addDefaultTerms().build(),
                                                           RdbDialect.mysql());
        DatabaseOperator postgresql = DatabaseOperator.create(new RecordingSqlExecutor(),
                                                                SqlRenderer.builder().addDefaultTerms().build(),
                                                                RdbDialect.postgresql());
        DatabaseOperator sqlServer = DatabaseOperator.create(new RecordingSqlExecutor(),
                                                                SqlRenderer.builder().addDefaultTerms().build(),
                                                                RdbDialect.sqlServer());

        assertAll(
                () -> mysql.unsafeNativeSql("show create table users"),
                () -> mysql.unsafeNativeSql("explain users"),
                () -> mysql.unsafeNativeSql("describe select * from users"),
                () -> mysql.unsafeNativeSql("explain table users"),
                () -> mysql.unsafeNativeSql("explain analyze select * from users"),
                () -> mysql.unsafeNativeSql("explain format=json select * from users"),
                () -> mysql.unsafeNativeSql("insert into archived_users(id) "
                                                     + "with ids as (select id from users) select id from ids"),
                () -> mysql.unsafeNativeSql("replace into archived_users(id) "
                                                     + "with ids as (select id from users) select id from ids"),
                () -> mysql.unsafeNativeSql("create table archived_users values row(1)"),
                () -> mysql.unsafeNativeSql("create database app character set utf8mb4"),
                () -> mysql.unsafeNativeSql("create table archived_users replace as select * from users"),
                () -> postgresql.unsafeNativeSql("create or replace view active_users as select id from users"),
                () -> postgresql.unsafeNativeSql("select 1 union values (2)"),
                () -> postgresql.unsafeNativeSql("values (1) union select 2"),
                () -> sqlServer.unsafeNativeSql("create or alter view active_users as select id from users"),
                () -> sqlServer.unsafeNativeSql("insert into #result exec dbo.load_users"),
                () -> sqlServer.unsafeNativeSql(
                        "select case when active = 1 then 1 else 0 end conversation from users"),
                () -> sqlServer.unsafeNativeSql("restore database app from disk = 'app.bak' with replace"));

        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.unsafeNativeSql(
                             "restore database app from disk = 'app.bak' with replace\ndrop table users"));
    }

    private static DatabaseOperator operator(ReactiveSqlExecutor executor) {
        return DatabaseOperator.create(executor,
                                       SqlRenderer.builder().addDefaultTerms().build(),
                                       RdbDialect.mysql());
    }

    private record UserRow(long id, String name) {
    }

    private static final class RecordingSqlExecutor implements ReactiveSqlExecutor {

        private final List<Map<String, Object>> rows = new ArrayList<>(List.of(row()));

        private SqlRequest request;

        private SqlExecutionOptions options;

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            this.request = request;
            return Flux.fromIterable(rows).map(DynamicRow::copyOf);
        }

        @Override
        public Flux<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
            this.request = request;
            this.options = options;
            return Flux.fromIterable(rows).map(DynamicRow::copyOf);
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            this.request = request;
            return Mono.just(1L);
        }

        private static Map<String, Object> row() {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", 1L);
            row.put("name", "Alice");
            return row;
        }
    }
}
