package com.flying.orm.rdb.bootstrap;

import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.join.JoinFieldRef;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.sql.DataSource;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

class JoinCustomFieldCodecTest {

    @TestFactory
    Stream<DynamicTest> aliasesPreserveTheRegisteredCodecOfEachSource() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(false, true).map(governed ->
                DynamicTest.dynamicTest("reactive=" + reactive + ", governed=" + governed, () -> {
                    Fixture fixture = fixture();
                    FlyingOrmClientBuilder builder = reactive
                            ? FlyingOrmClientBuilder.reactive(executor(), RdbDialect.h2())
                            : FlyingOrmClients.builder(dataSource()).configuredDialect("h2");
                    try (FlyingOrmClients clients = builder.entitySchema(fixture.account())
                            .entitySchema(fixture.audit()).build()) {
                        if (reactive) {
                            ReactiveFormClient forms = clients.forms().withFieldUsePolicy(fixture.policy(governed));
                            assertAll(
                                    () -> assertRows(forms.selectJoin(fixture.query()).collectList().block()),
                                    () -> assertPage(forms.pageJoin(fixture.query(), PageQuery.of(1, 10)).block()));
                        } else {
                            SyncFormClient forms = clients.syncForms().withFieldUsePolicy(fixture.policy(governed));
                            assertAll(
                                    () -> assertRows(forms.selectJoin(fixture.query())),
                                    () -> assertPage(forms.pageJoin(fixture.query(), PageQuery.of(1, 10))),
                                    () -> assertEquals(List.of(new AccountCode("A-17")),
                                            forms.selectJoinMapped(fixture.query(),
                                                    row -> (AccountCode) row.get("account_code"))));
                        }
                    }
                })));
    }

    private static void assertPage(PageResult<DynamicRow> page) {
        assertEquals(1L, page.total());
        assertRows(page.rows());
    }

    private static void assertRows(List<DynamicRow> rows) {
        assertEquals(1, rows.size());
        DynamicRow row = rows.getFirst();
        assertAll(
                () -> assertEquals(new AccountCode("A-17"), row.get("account_code")),
                () -> assertEquals(new AuditCode("E-9"), row.get("audit_code")),
                () -> assertEquals(new AccountCode("A-17"), row.get("account_code_again")));
    }

    private static Fixture fixture() {
        EntityTypeMappingRegistry mappings = EntityTypeMappingRegistry.builder()
                .register("account-code", AccountCode.class, DatabaseType.of("VARCHAR(32)"),
                        codec(AccountCode.class, AccountCode::value, AccountCode::new))
                .register("audit-code", AuditCode.class, DatabaseType.of("VARCHAR(32)"),
                        codec(AuditCode.class, AuditCode::value, AuditCode::new))
                .build();
        EntitySchemaDescriptor<Account> account = EntitySchemaDescriptor.builder(Account.class)
                .typeMappings(mappings).build();
        EntitySchemaDescriptor<Audit> audit = EntitySchemaDescriptor.builder(Audit.class)
                .typeMappings(mappings).build();
        JoinQuerySpec.Builder query = JoinQuerySpec.builder(account.form());
        JoinSource root = query.root();
        JoinSource joined = query.join(JoinType.LEFT, audit.form(), root, "id", "id");
        JoinQuerySpec spec = query.selectAs(root, "code", "account_code")
                .selectAs(joined, "code", "audit_code")
                .selectAs(root, "code", "account_code_again")
                .orderBy(root, "id", PageSort.Direction.ASC).build();
        FieldUsePolicy policy = FieldUsePolicy.builder()
                .allowJoin(new JoinFieldRef(root, "id"), FieldUse.JOIN, FieldUse.SORT)
                .allowJoin(new JoinFieldRef(joined, "id"), FieldUse.JOIN)
                .joinVisibility(new JoinFieldRef(root, "code"), FieldVisibility.FULL)
                .joinVisibility(new JoinFieldRef(joined, "code"), FieldVisibility.FULL)
                .build();
        return new Fixture(account, audit, spec, policy);
    }

    private static <T> ValueCodec codec(Class<T> type, Function<T, String> write, Function<String, T> read) {
        return new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == type;
            }

            @Override
            public Object write(Object value) {
                return value == null ? null : write.apply(type.cast(value));
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                return value == null ? null : read.apply(value.toString());
            }
        };
    }

    private static DynamicRow rawRow(String sql) {
        if (sql.startsWith("select count(*)")) {
            return DynamicRow.copyOf(Map.of("total", 1L));
        }
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("account_code", "A-17");
        values.put("audit_code", "E-9");
        values.put("account_code_again", "A-17");
        return DynamicRow.copyOf(values);
    }

    private static ReactiveSqlExecutor executor() {
        return new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.just(rawRow(request.sql()));
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                throw new UnsupportedOperationException("read-only fixture");
            }
        };
    }

    private static DataSource dataSource() {
        return proxy(DataSource.class, (instance, method, args) -> method.getName().equals("getConnection")
                ? proxy(Connection.class, (connection, operation, parameters) ->
                        operation.getName().equals("prepareStatement")
                                ? statement((String) parameters[0]) : defaultValue(operation.getReturnType()))
                : defaultValue(method.getReturnType()));
    }

    private static PreparedStatement statement(String sql) {
        return proxy(PreparedStatement.class, (instance, method, args) -> method.getName().equals("executeQuery")
                ? resultSet(rawRow(sql)) : defaultValue(method.getReturnType()));
    }

    private static ResultSet resultSet(DynamicRow row) {
        ResultSetMetaData metadata = proxy(ResultSetMetaData.class, (instance, method, args) ->
                switch (method.getName()) {
                    case "getColumnCount" -> row.columnCount();
                    case "getColumnLabel", "getColumnName" -> row.columnName((int) args[0] - 1);
                    default -> defaultValue(method.getReturnType());
                });
        AtomicBoolean beforeFirst = new AtomicBoolean(true);
        return proxy(ResultSet.class, (instance, method, args) -> switch (method.getName()) {
            case "getMetaData" -> metadata;
            case "next" -> beforeFirst.getAndSet(false);
            case "getObject" -> row.value((int) args[0] - 1);
            default -> defaultValue(method.getReturnType());
        });
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }

    private static Object defaultValue(Class<?> type) {
        return type.isPrimitive() && type != void.class ? Array.get(Array.newInstance(type, 1), 0) : null;
    }

    private record Fixture(EntitySchemaDescriptor<Account> account,
                           EntitySchemaDescriptor<Audit> audit,
                           JoinQuerySpec query,
                           FieldUsePolicy governedPolicy) {
        private FieldUsePolicy policy(boolean governed) {
            return governed ? governedPolicy : FieldUsePolicy.unrestricted();
        }
    }

    @TableName("accounts")
    private record Account(@TableId Long id, @TableColumn(databaseTypeId = "account-code") AccountCode code) {
    }

    @TableName("audits")
    private record Audit(@TableId Long id, @TableColumn(databaseTypeId = "audit-code") AuditCode code) {
    }

    private record AccountCode(String value) {
    }

    private record AuditCode(String value) {
    }
}
