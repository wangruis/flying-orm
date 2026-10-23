package com.flying.orm.consumer;

import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlRowLimitExceededException;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.operator.SqlTemplateOperator;
import com.flying.orm.rdb.operator.SyncSqlTemplateOperator;
import com.flying.orm.rdb.operator.SyncDatabaseOperator;
import com.flying.orm.rdb.template.SqlTemplate;
import com.flying.orm.rdb.template.SqlTemplateRegistry;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/** Public template calls execute the registered CTE/window SQL unchanged on both backends. */
class TemplateReportExecutionContractTest {
    private static final String SQL = """
            with totals as (
                select dept, count(*) as order_count, sum(coalesce(amount, 0)) as gross
                from ${table} where tenant_id = :tenant and status = :status
                group by dept having sum(coalesce(amount, 0)) >= :minimum
            ), ranked as (
                select dept, order_count, gross,
                       row_number() over (order by gross desc, dept) as rank_no from totals
            )
            select dept as "department", order_count as "orders", gross as "gross", rank_no as "rank"
            from ranked where rank_no <= :top order by rank_no
            """;

    @TestFactory
    Stream<DynamicTest> templateReportsKeepBindingsMappingAndServerContext() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest(
                reactive ? "R2DBC template report" : "JDBC template report", () -> verify(reactive)));
    }

    private static void verify(boolean reactive) throws Exception {
        String database = "template_report_" + UUID.randomUUID().toString().replace("-", "");
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + database);
        source.setUser("sa");
        source.setPassword("");
        ConnectionFactory factory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.DRIVER, "h2").option(ConnectionFactoryOptions.PROTOCOL, "mem")
                .option(ConnectionFactoryOptions.DATABASE, database).option(ConnectionFactoryOptions.USER, "sa")
                .option(ConnectionFactoryOptions.PASSWORD, "").build());
        AtomicLong syncTenant = new AtomicLong(7L);
        AtomicInteger providers = new AtomicInteger();
        SqlTemplateRegistry registry = SqlTemplateRegistry.builder()
                .register(SqlTemplate.query("ranked-sales", SQL, Set.of("table")), Set.of("tenant"))
                .register(SqlTemplate.query("nested-ranked-sales", SQL
                        .replace("as \"department\"", "as \"summary.name\"")
                        .replace("as \"orders\"", "as \"summary.count\"")
                        .replace("as \"gross\"", "as \"summary.amount\"")
                        .replace("as \"rank\"", "as \"position.position\""), Set.of("table")), Set.of("tenant"))
                .build();
        try (Connection keeper = source.getConnection(); Statement statement = keeper.createStatement();
             FlyingOrmClients clients = FlyingOrmClients.builder(ConnectionAccessTestSupport.jdbc(source),
                     ConnectionAccessTestSupport.reactive(factory)).configuredDialect("h2")
                     .sqlTemplates(registry)
                     .sqlTemplateParameterProvider((id, names) -> Mono.deferContextual(context -> {
                         providers.incrementAndGet();
                         return Mono.just(Map.of("tenant", context.<Long>get("tenant")));
                     }))
                     .syncSqlTemplateParameterProvider((id, names) -> {
                         providers.incrementAndGet();
                         return Map.of("tenant", syncTenant.get());
                     }).build()) {
            statement.execute("""
                    create table report_sales(tenant_id bigint, dept varchar(32),
                        amount decimal(12,2), status varchar(8))
                    """);
            statement.execute("""
                    insert into report_sales values
                        (7,'sales',10,'A'),(7,'sales',null,'A'),
                        (7,'engineering',20,'A'),(7,'engineering',15,'A'),(7,'engineering',500,'D'),
                        (7,'finance',4,'A'),(9,'sales',1000,'A'),(9,'engineering',70,'A')
                    """);
            if (reactive) {
                SqlTemplateOperator query = reactive(clients);
                assertThrows(IllegalArgumentException.class, () -> query.bind("tenant", 9L));
                Flux<Report> pending = query.query(Report.class);
                assertEquals(0, providers.get(), "trusted parameters must remain subscription-local");
                query.bind("minimum", 10000);
                assertReports(pending.contextWrite(context -> context.put("tenant", 7L)).collectList().block(), 7L);
                assertReports(pending.contextWrite(context -> context.put("tenant", 9L)).collectList().block(), 9L);
                assertEquals(2, providers.get());
                assertEquals("engineering", reactive(clients).bind("top", 1).one(Report.class)
                        .contextWrite(context -> context.put("tenant", 7L)).block().department());
                assertEquals("engineering", reactive(clients).bind("top", 1)
                        .one(row -> row.get("department")).contextWrite(context -> context.put("tenant", 7L)).block());
                assertNestedReport(reactive(clients).bind("top", 1).one(nestedMapper())
                        .contextWrite(context -> context.put("tenant", 7L)).block());
                assertNestedReport(clients.operator().sqlTemplate("nested-ranked-sales")
                        .identifier("table", "report_sales").bind("status", "A").bind("minimum", 5)
                        .bind("top", 1).one(RowMapper.nested(RankedSummary.class))
                        .contextWrite(context -> context.put("tenant", 7L)).block());
                assertNestedMap(clients.operator().sqlTemplate("nested-ranked-sales")
                        .identifier("table", "report_sales").bind("status", "A").bind("minimum", 5)
                        .bind("top", 1).one(RowMapper.nested())
                        .contextWrite(context -> context.put("tenant", 7L)).block());
                assertNull(reactive(clients).bind("minimum", 10000).one(Report.class)
                        .contextWrite(context -> context.put("tenant", 7L)).block());
                assertThrows(IndexOutOfBoundsException.class, () -> reactive(clients).one(Report.class)
                        .contextWrite(context -> context.put("tenant", 7L)).block());
                assertThrows(SqlRowLimitExceededException.class, () -> reactive(clients)
                        .options(SqlExecutionOptions.unlimited().withMaxRows(1)).query(Report.class)
                        .contextWrite(context -> context.put("tenant", 7L)).collectList().block());
                assertTrue(reactive(clients).bind("status", "A' or 1=1 --").query()
                        .contextWrite(context -> context.put("tenant", 7L)).collectList().block().isEmpty());
            } else {
                AtomicLong viewTenant = new AtomicLong(7L);
                AtomicInteger viewReads = new AtomicInteger();
                SyncDatabaseOperator view = clients.syncOperator().withSqlTemplates(
                        SqlTemplateRegistry.builder().register(SqlTemplate.query("view-sales", SQL, Set.of("table")),
                                Set.of("tenant")).build(),
                        (id, names) -> {
                            assertEquals("view-sales", id);
                            assertEquals(Set.of("tenant"), names);
                            viewReads.incrementAndGet();
                            return Map.of("tenant", viewTenant.get());
                        });
                SyncSqlTemplateOperator viewQuery = view.sqlTemplate("view-sales").identifier("table", "report_sales")
                        .bindAll(Map.of("status", "A", "minimum", 5, "top", 2));
                assertEquals(0, viewReads.get());
                assertThrows(IllegalArgumentException.class, () -> viewQuery.bind("tenant", 9L));
                assertReports(viewQuery.query(Report.class), 7L);
                viewTenant.set(9L);
                assertReports(viewQuery.query(Report.class), 9L);
                assertEquals(2, viewReads.get());
                assertEquals(0, providers.get(), "template view must not call the original provider");
                assertThrows(IllegalArgumentException.class, () -> clients.syncOperator().sqlTemplate("view-sales"));
                SyncSqlTemplateOperator query = sync(clients);
                assertThrows(IllegalArgumentException.class, () -> query.bind("tenant", 9L));
                assertEquals(0, providers.get());
                assertReports(query.query(Report.class), 7L);
                syncTenant.set(9L);
                assertReports(query.query(Report.class), 9L);
                assertEquals(2, providers.get());
                syncTenant.set(7L);
                assertEquals("engineering", sync(clients).bind("top", 1).one(Report.class).department());
                assertEquals("engineering", sync(clients).bind("top", 1).one(row -> row.get("department")));
                assertNestedReport(sync(clients).bind("top", 1).one(nestedMapper()));
                assertNestedReport(clients.syncOperator().sqlTemplate("nested-ranked-sales")
                        .identifier("table", "report_sales").bind("status", "A").bind("minimum", 5)
                        .bind("top", 1).one(RowMapper.nested(RankedSummary.class)));
                assertNestedMap(clients.syncOperator().sqlTemplate("nested-ranked-sales")
                        .identifier("table", "report_sales").bind("status", "A").bind("minimum", 5)
                        .bind("top", 1).one(RowMapper.nested()));
                assertNull(sync(clients).bind("minimum", 10000).one(Report.class));
                assertThrows(IllegalStateException.class, () -> sync(clients).one(Report.class));
                assertThrows(SqlRowLimitExceededException.class, () -> sync(clients)
                        .options(SqlExecutionOptions.unlimited().withMaxRows(1)).query(Report.class));
                assertTrue(sync(clients).bind("status", "A' or 1=1 --").query().isEmpty());
            }
        }
    }

    private static SqlTemplateOperator reactive(FlyingOrmClients clients) {
        return clients.operator().sqlTemplate("ranked-sales").identifier("table", "report_sales")
                .bindAll(Map.of("status", "A", "minimum", 5, "top", 2));
    }

    private static SyncSqlTemplateOperator sync(FlyingOrmClients clients) {
        return clients.syncOperator().sqlTemplate("ranked-sales").identifier("table", "report_sales")
                .bindAll(Map.of("status", "A", "minimum", 5, "top", 2));
    }

    private static void assertReports(List<Report> rows, long tenant) {
        assertNotNull(rows);
        assertEquals(2, rows.size());
        List<Report> expected = tenant == 7L
                ? List.of(new Report("engineering", 2L, new BigDecimal("35.00"), 1L),
                          new Report("sales", 2L, new BigDecimal("10.00"), 2L))
                : List.of(new Report("sales", 1L, new BigDecimal("1000.00"), 1L),
                          new Report("engineering", 1L, new BigDecimal("70.00"), 2L));
        assertEquals(expected, rows);
    }

    private static RowMapper<RankedSummary> nestedMapper() {
        return RowMapper.nested(RankedSummary.class).withAliases(Map.of(
                "department", "summary.name", "orders", "summary.count",
                "gross", "summary.amount", "rank", "position.position"));
    }

    private static void assertNestedReport(RankedSummary result) {
        assertEquals(new RankedSummary(new Summary("engineering", 2L, new BigDecimal("35.00")),
                new Position(1L)), result);
    }

    private static void assertNestedMap(Map<String, Object> result) {
        assertEquals(Map.of("summary", Map.of("name", "engineering", "count", 2L,
                "amount", new BigDecimal("35.00")), "position", Map.of("position", 1L)), result);
    }

    public record Report(String department, Long orders, BigDecimal gross, Long rank) { }
    public record Summary(String name, Long count, BigDecimal amount) { }
    public record Position(Long position) { }
    public record RankedSummary(Summary summary, Position position) { }
}
