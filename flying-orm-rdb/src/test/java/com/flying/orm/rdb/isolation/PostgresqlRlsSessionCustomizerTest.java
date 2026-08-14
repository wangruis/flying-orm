package com.flying.orm.rdb.isolation;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PostgresqlRlsSessionCustomizerTest {

    @Test
    void bindsOneSchemaAndQuotesItsIdentifierSemanticsInsidePostgresql() {
        List<String> sql = new ArrayList<>();
        List<Object> bindings = new ArrayList<>();
        Connection connection = connection(sql, bindings);
        IsolationContext context = new IsolationContext("tenant-7", null, "TenantSchema", Map.of());

        StepVerifier.create(new PostgresqlRlsSessionCustomizer().initialize(connection, context))
                    .verifyComplete();

        assertEquals(List.of("select set_config('search_path', quote_ident($1), false)"), sql);
        assertEquals(List.of("TenantSchema"), bindings);
    }

    private static Connection connection(List<String> sql, List<Object> bindings) {
        return (Connection) Proxy.newProxyInstance(
                PostgresqlRlsSessionCustomizerTest.class.getClassLoader(), new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> {
                    if ("createStatement".equals(method.getName())) {
                        sql.add((String) arguments[0]);
                        return statement(bindings);
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Statement statement(List<Object> bindings) {
        final Statement[] reference = new Statement[1];
        reference[0] = (Statement) Proxy.newProxyInstance(
                PostgresqlRlsSessionCustomizerTest.class.getClassLoader(), new Class<?>[]{Statement.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "bind" -> {
                        bindings.add(arguments[1]);
                        yield reference[0];
                    }
                    case "execute" -> Mono.just(result());
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        return reference[0];
    }

    private static Result result() {
        return (Result) Proxy.newProxyInstance(
                PostgresqlRlsSessionCustomizerTest.class.getClassLoader(), new Class<?>[]{Result.class},
                (proxy, method, arguments) -> {
                    if ("map".equals(method.getName())) {
                        return Mono.just(Boolean.TRUE);
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }
}
