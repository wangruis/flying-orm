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

        assertThrows(IllegalArgumentException.class,
                     () -> operator.unsafeNativeSql("select * from users where id = :id").toRequest());
        assertThrows(IllegalArgumentException.class,
                     () -> operator.unsafeNativeSql("select * from users").bind("unused", 1).toRequest());
        assertThrows(IllegalArgumentException.class,
                     () -> operator.unsafeNativeSql("select 1; delete from users"));
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
