package com.flying.orm.testkit.dialect;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.isolation.IsolationContext;
import com.flying.orm.rdb.isolation.IsolationContexts;
import com.flying.orm.rdb.isolation.PostgresqlRlsSessionCustomizer;
import com.flying.orm.rdb.isolation.R2dbcSessionCustomizer;
import com.flying.orm.rdb.isolation.RoutingConnectionFactory;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import com.flying.orm.rdb.vector.VectorValueCodec;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.Result;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PostgreSQL 最终认证里的高风险场景：pgvector 真正执行，以及连接池复用时 schema/RLS 会话是否会串租户。
 *
 * <p>测试只有在配置 {@code flying.orm.compat.postgresql.url} 后才运行。表、schema 和策略名称固定且只用于
 * 本地认证，每次开始前都会清理旧现场，失败现场则保留到下一次执行前，方便直接进容器排查。</p>
 *
 * @author wangr
 * @date 2026-08-02
 * @version v1.0
 */
class ExternalR2dbcPostgresqlAdvancedCompatibilityTest {

    private static final String URL_PROPERTY = "flying.orm.compat.postgresql.url";

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private static final String RLS_CERTIFICATION_ROLE = "flying_orm_rls_cert";

    /**
     * 这条用真实 vector 列验证完整链路，不只检查 SQL 文本：扩展安装、float[] 参数绑定、驱动回读和距离排序
     * 任意一环不兼容都会在这里暴露。
     */
    @Test
    void executesPgvectorWriteReadAndNearestNeighbourWhenUrlIsConfigured() {
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(ConnectionFactories.get(configuredUrl()));
        cleanupVector(executor);

        Mono<Void> scenario = execute(executor, "create extension if not exists vector")
                .then(execute(executor,
                              "create table flying_orm_vector_cert ("
                                      + "id bigint primary key, embedding vector(3) not null)"))
                .then(executor.rowsUpdated(new SqlRequest(
                        "insert into flying_orm_vector_cert(id, embedding) values (?, cast(? as vector)), "
                                + "(?, cast(? as vector))",
                        List.of(1L, VectorValueCodec.write(List.of(1, 0, 0), 3),
                                2L, VectorValueCodec.write(List.of(0, 1, 0), 3)))))
                .doOnNext(rows -> assertEquals(2L, rows))
                .thenMany(executor.query(new SqlRequest(
                        "select id, embedding, embedding <-> cast(? as vector) as distance "
                                + "from flying_orm_vector_cert order by distance limit 1",
                        List.of(VectorValueCodec.write(List.of(0.9, 0.1, 0), 3)))))
                .single()
                .doOnNext(row -> {
                    assertEquals(1L, number(row, "id").longValue());
                    assertArrayEquals(new float[]{1F, 0F, 0F},
                                      VectorValueCodec.read(value(row, "embedding"), 3));
                    assertTrue(number(row, "distance").doubleValue() < 0.2D);
                })
                .then();

        try {
            scenario.block(TIMEOUT);
        } finally {
            cleanupVector(executor);
        }
    }

    /**
     * 池大小固定为 1，两个 schema 和两个 RLS 租户必然复用同一条物理连接。每次查询后再绕过路由直接借连接，
     * 可以确认 close 前的 reset 已经完成，而不是只验证下一次 initialize 恰好覆盖了旧值。
     */
    @Test
    void resetsSchemaAndRlsBeforeReturningConnectionToPoolWhenUrlIsConfigured() {
        ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration
                .builder(ConnectionFactories.get(configuredUrl()))
                .name("flying-orm-postgresql-isolation-cert")
                .initialSize(1)
                .minIdle(1)
                .maxSize(1)
                .maxAcquireTime(Duration.ofSeconds(5))
                .maxIdleTime(Duration.ofMinutes(1))
                .build());
        R2dbcSqlExecutor direct = R2dbcSqlExecutor.create(pool);
        RoutingConnectionFactory routing = new RoutingConnectionFactory(pool, ignored -> pool, rlsCustomizer());
        R2dbcSqlExecutor isolated = R2dbcSqlExecutor.create(routing);

        try {
            setupIsolation(direct).block(TIMEOUT);

            assertEquals("schema-a", schemaMarker(isolated, "tenant_a", "tenant-a"));
            assertEquals("schema-b", schemaMarker(isolated, "tenant_b", "tenant-b"));
            assertEquals(List.of("tenant-a"), visibleTenants(isolated, "tenant-a"));
            assertEquals(List.of("tenant-b"), visibleTenants(isolated, "tenant-b"));
            assertTrue(visibleTenantsWithoutContext(isolated).isEmpty(),
                       "没有隔离上下文时，普通业务角色不应该看到任何租户数据");

            Map<String, Object> cleanSession = direct.query(SqlRequest.nativeSql(
                            "select current_schema() as schema_name, "
                                    + "current_setting('app.tenant_id', true) as tenant_value, "
                                    + "session_user as session_user_name, current_user as current_user_name",
                            List.of()))
                    .single()
                    .block(TIMEOUT);
            assertEquals("public", text(cleanSession, "schema_name"));
            assertEquals(text(cleanSession, "session_user_name"), text(cleanSession, "current_user_name"),
                         "连接归还池前必须恢复原始数据库身份，不能把认证用普通角色带给下一次借用");
            String tenantValue = text(cleanSession, "tenant_value");
            assertTrue(tenantValue == null || tenantValue.isEmpty(),
                       "连接归还池后仍残留 RLS tenant 值：" + tenantValue);
        } finally {
            cleanupIsolation(direct);
            pool.disposeLater().onErrorResume(ignored -> Mono.empty()).block(TIMEOUT);
        }
    }

    private static String schemaMarker(R2dbcSqlExecutor executor, String schema, String tenant) {
        IsolationContext context = IsolationContext.shared()
                                                   .withSchema(schema)
                                                   .withRlsSettings(Map.of("app.tenant_id", tenant));
        Map<String, Object> row = IsolationContexts.with(
                        executor.query(SqlRequest.nativeSql("select marker from scoped_item", List.of())).single(),
                        context)
                .block(TIMEOUT);
        return text(row, "marker");
    }

    private static List<String> visibleTenants(R2dbcSqlExecutor executor, String tenant) {
        IsolationContext context = IsolationContext.shared()
                                                   .withRlsSettings(Map.of("app.tenant_id", tenant));
        return IsolationContexts.with(
                        executor.query(SqlRequest.nativeSql(
                                        "select tenant_id from public.flying_orm_rls_cert order by tenant_id",
                                        List.of()))
                                .map(row -> text(row, "tenant_id"))
                                .collectList(),
                        context)
                .block(TIMEOUT);
    }

    private static List<String> visibleTenantsWithoutContext(R2dbcSqlExecutor executor) {
        return IsolationContexts.with(
                        executor.query(SqlRequest.nativeSql(
                                        "select tenant_id from public.flying_orm_rls_cert order by tenant_id",
                                        List.of()))
                                .map(row -> text(row, "tenant_id"))
                                .collectList(),
                        IsolationContext.shared())
                .block(TIMEOUT);
    }

    private static Mono<Void> setupIsolation(R2dbcSqlExecutor executor) {
        cleanupIsolation(executor);
        // PostgreSQL 超级用户始终绕过 RLS。认证容器的初始化账号是超级用户，所以先准备一个
        // NOLOGIN 普通角色，查询时只临时切换数据库身份，才能验证真实的策略过滤和连接清理。
        return execute(executor,
                       "do $$ begin if not exists (select 1 from pg_roles where rolname = '"
                               + RLS_CERTIFICATION_ROLE + "') then create role " + RLS_CERTIFICATION_ROLE
                               + " nologin; end if; end $$")
                .then(execute(executor, "create schema tenant_a"))
                .then(execute(executor, "create schema tenant_b"))
                .then(execute(executor, "create table tenant_a.scoped_item(marker varchar(32) not null)"))
                .then(execute(executor, "create table tenant_b.scoped_item(marker varchar(32) not null)"))
                .then(execute(executor, "insert into tenant_a.scoped_item(marker) values ('schema-a')"))
                .then(execute(executor, "insert into tenant_b.scoped_item(marker) values ('schema-b')"))
                .then(execute(executor,
                              "create table public.flying_orm_rls_cert("
                                      + "tenant_id varchar(32) not null, payload varchar(32) not null)"))
                .then(execute(executor,
                              "insert into public.flying_orm_rls_cert(tenant_id, payload) values "
                                      + "('tenant-a', 'a'), ('tenant-b', 'b')"))
                .then(execute(executor, "alter table public.flying_orm_rls_cert enable row level security"))
                .then(execute(executor, "alter table public.flying_orm_rls_cert force row level security"))
                .then(execute(executor,
                              "create policy flying_orm_tenant_policy on public.flying_orm_rls_cert "
                                      + "using (tenant_id = nullif(current_setting('app.tenant_id', true), ''))"))
                .then(execute(executor,
                              "grant usage on schema public, tenant_a, tenant_b to " + RLS_CERTIFICATION_ROLE))
                .then(execute(executor,
                              "grant select on public.flying_orm_rls_cert, tenant_a.scoped_item, "
                                      + "tenant_b.scoped_item to " + RLS_CERTIFICATION_ROLE));
    }

    /**
     * 认证容器使用超级用户建表，但 RLS 必须以普通角色执行才有意义。这里的包装器只属于测试：
     * 借出连接后先切换普通角色，再交给正式的 PostgreSQL 隔离定制器设置 schema 和 RLS 变量；
     * 归还时顺序反过来，确保同一条物理连接回到池里时既没有租户变量，也没有残留角色。
     */
    private static R2dbcSessionCustomizer rlsCustomizer() {
        PostgresqlRlsSessionCustomizer delegate = new PostgresqlRlsSessionCustomizer();
        return new R2dbcSessionCustomizer() {
            @Override
            public Mono<Void> initialize(Connection connection, IsolationContext context) {
                return execute(connection, "set session authorization " + RLS_CERTIFICATION_ROLE)
                        .then(Mono.from(delegate.initialize(connection, context)));
            }

            @Override
            public Mono<Void> reset(Connection connection, IsolationContext context) {
                Mono<Void> restoreIdentity = execute(connection, "reset session authorization");
                return Mono.from(delegate.reset(connection, context))
                           .then(restoreIdentity)
                           // 即使某个变量清理失败，也要尽力恢复数据库身份，避免污染池里的连接。
                           .onErrorResume(error -> execute(connection, "reset session authorization")
                                   .then(Mono.error(error)));
            }
        };
    }

    private static void cleanupVector(R2dbcSqlExecutor executor) {
        execute(executor, "drop table if exists flying_orm_vector_cert")
                .onErrorResume(ignored -> Mono.empty())
                .block(TIMEOUT);
    }

    private static void cleanupIsolation(R2dbcSqlExecutor executor) {
        execute(executor, "drop table if exists public.flying_orm_rls_cert")
                .then(execute(executor, "drop schema if exists tenant_a cascade"))
                .then(execute(executor, "drop schema if exists tenant_b cascade"))
                .onErrorResume(ignored -> Mono.empty())
                .block(TIMEOUT);
    }

    private static Mono<Void> execute(R2dbcSqlExecutor executor, String sql) {
        return executor.rowsUpdated(SqlRequest.nativeSql(sql, List.of())).then();
    }

    private static Mono<Void> execute(Connection connection, String sql) {
        return Flux.from(connection.createStatement(sql).execute())
                   .flatMap(Result::getRowsUpdated)
                   .then();
    }

    private static String configuredUrl() {
        String url = System.getProperty(URL_PROPERTY);
        Assumptions.assumeTrue(url != null && !url.isBlank(), URL_PROPERTY + " is not configured");
        return url;
    }

    private static Number number(Map<String, Object> row, String column) {
        return (Number) value(row, column);
    }

    private static String text(Map<String, Object> row, String column) {
        Object value = value(row, column);
        return value == null ? null : value.toString();
    }

    private static Object value(Map<String, Object> row, String column) {
        return row.entrySet()
                  .stream()
                  .filter(entry -> entry.getKey().equalsIgnoreCase(column))
                  .map(Map.Entry::getValue)
                  .findFirst()
                  .orElse(null);
    }
}
