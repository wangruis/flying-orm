package com.flying.orm.rdb.bootstrap;

import com.flying.orm.rdb.jdbc.JdbcConnectionAccess;
import com.flying.orm.rdb.reactive.R2dbcConnectionAccess;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class ConnectionAccessBootstrapBoundaryTest {
    @Test
    void nativeBuildersAcceptOnlyUpperConnectionAccessPorts() {
        List<List<Class<?>>> parameters = Arrays.stream(FlyingOrmClients.class.getDeclaredMethods())
                .filter(method -> method.getName().equals("builder"))
                .map(method -> List.of(method.getParameterTypes())).toList();
        assertEquals(3, parameters.size());
        assertEquals(Set.of(List.of(JdbcConnectionAccess.class), List.of(R2dbcConnectionAccess.class),
                            List.of(JdbcConnectionAccess.class, R2dbcConnectionAccess.class)),
                     Set.copyOf(parameters));
    }

    @Test
    void builderHasNoTransactionConfiguration() {
        assertFalse(Arrays.stream(FlyingOrmClientBuilder.class.getDeclaredMethods())
                .map(Method::getName).anyMatch(name -> name.toLowerCase().contains("transaction")));
    }

    @Test
    void environmentAndAssemblyDoNotRetainTransactionOrInfrastructureOwners() {
        for (Class<?> type : List.of(FlyingOrmEnvironment.class, FlyingOrmAssemblyRequest.class,
                                    FlyingOrmClientBuilderSupport.class)) {
            assertFalse(Arrays.stream(type.getDeclaredFields()).anyMatch(field -> {
                String name = field.getName().toLowerCase();
                String fieldType = field.getGenericType().getTypeName();
                return name.contains("transaction") || fieldType.contains("javax.sql.DataSource")
                        || fieldType.contains("io.r2dbc.spi.ConnectionFactory");
            }), type.getName());
        }
    }
}
