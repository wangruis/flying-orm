package com.flying.orm.rdb.dialect;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RdbDialectResolverTopologyBoundaryTest {

    @Test
    void configuredDialectDoesNotReadRuntimeOrPhysicalMetadata() {
        AtomicInteger runtimeMetadataReads = new AtomicInteger();
        AtomicInteger physicalMetadataReads = new AtomicInteger();
        ConnectionFactory runtime = connectionFactory("H2", runtimeMetadataReads);
        ConnectionFactory physical = connectionFactory("PostgreSQL", physicalMetadataReads);

        RdbDialect dialect = RdbDialectResolver.resolveAndValidate(
                "h2", runtime, Map.of("unavailable-shard", physical));

        assertEquals("h2", dialect.name());
        assertEquals(0, runtimeMetadataReads.get());
        assertEquals(0, physicalMetadataReads.get());
    }

    @Test
    void missingConfiguredDialectReadsOnlyTheUnifiedRuntimeMetadataOnce() {
        AtomicInteger runtimeMetadataReads = new AtomicInteger();

        RdbDialect dialect = RdbDialectResolver.resolveAndValidate(
                null, connectionFactory("H2", runtimeMetadataReads), Map.of());

        assertEquals("h2", dialect.name());
        assertEquals(1, runtimeMetadataReads.get());
    }

    private static ConnectionFactory connectionFactory(String productName, AtomicInteger metadataReads) {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                throw new AssertionError("dialect detection must not create an R2DBC connection");
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                metadataReads.incrementAndGet();
                return () -> productName;
            }
        };
    }
}
