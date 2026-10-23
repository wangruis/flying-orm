package com.flying.orm.consumer;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DialectCapabilityId;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.schema.RelationalSchemaSqlRenderer;
import com.flying.orm.rdb.schema.SchemaDialect;
import com.flying.orm.rdb.schema.SchemaOperation;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class DialectExtensionContractTest {
    @Test
    void customizingDdlPreservesEveryOtherDialectComponentAndBuiltinSyntax() {
        RelationalTableDefinition table = RelationalTableDefinition.builder(RelationIdentity.table("items"))
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false)
                        .generation(ValueGeneration.identity()).build())
                .primaryKey(PrimaryKeyDefinition.of("pk_items", "id"))
                .addColumn(ColumnDefinition.builder("title", "VARCHAR").length(80).comment("title").build())
                .comment("items").build();
        SchemaOperation create = create(table);
        for (RdbDialect base : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer(), RdbDialect.h2())) {
            AtomicInteger calls = new AtomicInteger();
            RdbDialect customized = base.withSchema(base.schema().withOperationRenderer((operation, defaults) -> {
                calls.incrementAndGet();
                return defaults.get();
            }));
            assertNotSame(base, customized);
            assertNotSame(base.schema(), customized.schema());
            assertEquals(base.name(), customized.name());
            assertEquals(base.version(), customized.version());
            assertEquals(base.maxIdentifierLength(), customized.maxIdentifierLength());
            assertSame(base.capabilities(), customized.capabilities());
            assertSame(base.pagination(), customized.pagination());
            assertSame(base.upsert(), customized.upsert());
            assertSame(base.json(), customized.json());
            assertSame(base.lockingReadDialect(), customized.lockingReadDialect());
            assertEquals(base.capabilities().ids(), customized.capabilities().ids());
            List<SqlRequest> expected = RelationalSchemaSqlRenderer.create(base.schema()).render(create);
            List<SqlRequest> actual = RelationalSchemaSqlRenderer.create(customized.schema()).render(create);
            assertEquals(expected, actual, base.name());
            assertEquals(1, calls.get());
            RelationalSchemaSqlRenderer.create(base.schema()).render(create);
            assertEquals(1, calls.get(), "the original dialect must not be mutated");
        }
    }

    @Test
    void replacementKeepsCustomTypeRulesAndDoesNotStackOldCallbacks() {
        AtomicInteger oldCalls = new AtomicInteger();
        SchemaDialect original = SchemaDialect.builder().quoteIdentifiers('"')
                .mapType("VARCHAR", type -> "TEXT")
                .operationRenderer((operation, defaults) -> {
                    oldCalls.incrementAndGet();
                    return defaults.get();
                }).build();
        AtomicInteger newCalls = new AtomicInteger();
        SchemaDialect replacement = original.withOperationRenderer((operation, defaults) -> {
            newCalls.incrementAndGet();
            assertSame(defaults.get(), defaults.get());
            return defaults.get();
        });
        SchemaOperation create = create(RelationalTableDefinition.builder(RelationIdentity.table("items"))
                .addColumn(ColumnDefinition.builder("title", "VARCHAR").build()).build());
        List<SqlRequest> requests = RelationalSchemaSqlRenderer.create(replacement).render(create);
        assertEquals("create table \"items\" (\"title\" TEXT)", requests.getFirst().sql());
        assertEquals(0, oldCalls.get());
        assertEquals(1, newCalls.get());
        assertEquals(requests, RelationalSchemaSqlRenderer.create(original).render(create));
        assertEquals(1, oldCalls.get());
    }

    private static SchemaOperation create(RelationalTableDefinition table) {
        return SchemaOperation.of(SchemaOperation.Kind.CREATE_TABLE, table.identity(), table.identity().table(),
                null, table, SchemaOperation.Compatibility.SAFE_INCREMENTAL);
    }
}
