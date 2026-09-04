package com.flying.orm.rdb.schema;

import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class EntityCollectionDeterministicOrderTest {

    @Test
    void plansOnlyTheExplicitEntityCollectionInStableOrder() {
        DatabaseDescriptor database = DatabaseDescriptor.of(
                "test", "1", "test", DialectCapabilities.empty());
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntitySchemaSynchronizer synchronizer = new EntitySchemaSynchronizer(
                    models, null, null, null, null);

            MultiTableSchemaPlanner.Plan first = synchronizer.plan(
                    database,
                    List.of(ZetaRecord.class, AlphaRecord.class),
                    MultiTableSchemaPlanner.ForeignKeyCycleSupport.SUPPORTED);
            MultiTableSchemaPlanner.Plan second = synchronizer.plan(
                    database,
                    List.of(AlphaRecord.class, ZetaRecord.class),
                    MultiTableSchemaPlanner.ForeignKeyCycleSupport.SUPPORTED);

            assertSame(database, first.database());
            assertEquals(signatures(first), signatures(second));
            assertEquals(List.of("CREATE_TABLE:alpha_records", "CREATE_TABLE:zeta_records"),
                         signatures(first));
        }
    }

    private static List<String> signatures(MultiTableSchemaPlanner.Plan plan) {
        return plan.operations().stream()
                .map(operation -> operation.kind() + ":" + operation.relation().table())
                .toList();
    }

    @TableName("alpha_records")
    private static final class AlphaRecord {

        @TableId
        private Long id;
    }

    @TableName("zeta_records")
    private static final class ZetaRecord {

        @TableId
        private Long id;
    }

    @TableName("unlisted_records")
    private static final class UnlistedRecord {

        @TableId
        private Long id;
    }
}
