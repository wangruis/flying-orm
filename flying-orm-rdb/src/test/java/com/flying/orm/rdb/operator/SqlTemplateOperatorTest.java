package com.flying.orm.rdb.operator;

import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.template.SqlTemplate;
import com.flying.orm.rdb.template.SqlTemplateParameterProvider;
import com.flying.orm.rdb.template.SqlTemplateRegistry;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证注册 SQL 可以直接承载复杂报表，同时不为报表再建一套执行器和结果映射。
 *
 * @author wangr
 * @date 2026-08-04
 * @version v1.0
 */
class SqlTemplateOperatorTest {

    /** 服务端安全参数必须在订阅时读取，普通调用方只能绑定模板剩余的业务参数。 */
    @Test
    void executesRegisteredQueryWithSubscriptionTimeServerParameters() {
        RecordingExecutor executor = new RecordingExecutor();
        SqlTemplateRegistry templates = SqlTemplateRegistry.builder()
                .register(SqlTemplate.query(
                        "monthly-sales",
                        "with totals as (select tenant_id, sum(amount) total from sales "
                                + "where tenant_id = :tenantId and created_at >= :startTime group by tenant_id) "
                                + "select tenant_id, total from totals",
                        Set.of()),
                          Set.of("tenantId"))
                .build();
        SqlTemplateParameterProvider parameters = (templateId, names) -> Mono.deferContextual(context ->
                Mono.just(Map.of("tenantId", context.get("tenantId"))));
        DatabaseOperator operator = operator(executor).withSqlTemplates(templates, parameters);
        Instant startTime = Instant.parse("2026-08-01T00:00:00Z");

        StepVerifier.create(operator.sqlTemplate("monthly-sales")
                                    .bind("startTime", startTime)
                                    .query()
                                    .contextWrite(context -> context.put("tenantId", "tenant-a")))
                    .expectNext(DynamicRow.copyOf(Map.of("tenant_id", "tenant-a", "total", 12L)))
                    .verifyComplete();

        assertEquals(List.of("tenant-a", startTime), executor.request.parameters());
        assertEquals(true, executor.request.sql().startsWith("with totals as"));
    }

    /** 安全参数不能由普通 bind 伪造，提供器少传或多传参数也要在获取连接前失败。 */
    @Test
    void rejectsForgedAndInvalidServerParametersBeforeExecution() {
        RecordingExecutor executor = new RecordingExecutor();
        SqlTemplateRegistry templates = SqlTemplateRegistry.builder()
                .register(SqlTemplate.query("tenant-users",
                                            "select id from users where tenant_id = :tenantId",
                                            Set.of()),
                          Set.of("tenantId"))
                .build();

        DatabaseOperator missing = operator(executor).withSqlTemplates(
                templates,
                (templateId, names) -> Mono.just(Map.of()));
        IllegalArgumentException forged = assertThrows(
                IllegalArgumentException.class,
                () -> missing.sqlTemplate("tenant-users").bind("tenantId", "forged"));
        assertFalse(forged.getMessage().contains("tenantId"));
        StepVerifier.create(missing.sqlTemplate("tenant-users").query())
                    .expectErrorSatisfies(error -> {
                        assertEquals(IllegalArgumentException.class, error.getClass());
                        assertFalse(error.getMessage().contains("tenantId"));
                    })
                    .verify();
        assertNull(executor.request);

        DatabaseOperator extra = operator(executor).withSqlTemplates(
                templates,
                (templateId, names) -> Mono.just(Map.of("tenantId", "tenant-a", "userId", "u-1")));
        StepVerifier.create(extra.sqlTemplate("tenant-users").query())
                    .expectErrorSatisfies(error -> {
                        assertEquals(IllegalArgumentException.class, error.getClass());
                        assertFalse(error.getMessage().contains("tenantId"));
                        assertFalse(error.getMessage().contains("userId"));
                    })
                    .verify();
        assertNull(executor.request);
    }

    /** 服务端参数提供器为空时，公开失败文本不能回显无长度上限的注册模板 ID。 */
    @Test
    void doesNotExposeAnUnboundedTemplateIdWhenTheServerParameterProviderIsEmpty() {
        String templateId = "report-" + "s".repeat(5_000);
        SqlTemplateRegistry templates = SqlTemplateRegistry.builder()
                .register(SqlTemplate.query(templateId,
                                            "select id from users where tenant_id = :tenantId",
                                            Set.of()),
                          Set.of("tenantId"))
                .build();
        DatabaseOperator operator = operator(new RecordingExecutor()).withSqlTemplates(
                templates, (id, names) -> Mono.empty());

        StepVerifier.create(operator.sqlTemplate(templateId).query())
                    .expectErrorSatisfies(error -> {
                        assertEquals(IllegalArgumentException.class, error.getClass());
                        assertFalse(error.getMessage().contains(templateId));
                    })
                    .verify();
    }

    /** 同步门面使用原生 JDBC 同步执行链，模板选择、参数合并和 SQL 编译仍复用统一规则。 */
    private static DatabaseOperator operator(RecordingExecutor executor) {
        return DatabaseOperator.create(executor,
                                       SqlRenderer.builder().addDefaultTerms().build(),
                                       RdbDialect.mysql());
    }

    /** 只记录最终请求，测试关注的是连接获取前的模板与安全参数行为。 */
    private static final class RecordingExecutor implements ReactiveSqlExecutor {

        private SqlRequest request;

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            this.request = request;
            return Flux.just(DynamicRow.copyOf(Map.of("tenant_id", "tenant-a", "total", 12L)));
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            this.request = request;
            return Mono.just(1L);
        }
    }
}
