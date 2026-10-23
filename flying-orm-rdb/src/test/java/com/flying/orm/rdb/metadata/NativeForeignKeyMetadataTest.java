package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.ReferentialAction;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NativeForeignKeyMetadataTest {

    @Test
    void postgresDoesNotTreatPartialDeleteActionsAsWholeForeignKeyActions() {
        for (String schema : new String[]{null, "app"}) {
            String sql = PostgreSqlMetadataQueries.queries().foreignKeyQuery().create(schema, "orders").sql();
            // JSON access preserves compatibility with catalogs predating confdelsetcols.
            assertTrue(sql.contains("pg_catalog.to_jsonb(con)->>'confdelsetcols' is null"));
            // Containment accepts an explicit list of every key column in either order, not a subset.
            assertTrue(sql.contains("pg_catalog.to_jsonb(con)->'confdelsetcols' @> pg_catalog.to_jsonb(con.conkey)"));
        }
    }

    @Test
    void nativeForeignKeyQueriesProjectSourceAndReferencedSchema() {
        List<MetadataQueryProfile.Queries> readers = List.of(
                H2MetadataQueries.queries(),
                MySqlMetadataQueries.queries(),
                PostgreSqlMetadataQueries.queries(),
                OracleMetadataQueries.queries(),
                SqlServerMetadataQueries.queries());

        assertTrue(readers.stream().allMatch(queries -> {
            String sql = queries.foreignKeyQuery().create(null, "orders").sql();
            return sql.contains("TABLE_SCHEMA") && sql.contains("REFERENCED_TABLE_SCHEMA");
        }));
    }

    @Test
    void mysqlIndexQueryDoesNotHideAnIndexOnlyBecauseItsNameMatchesAForeignKey() {
        String sql = MySqlMetadataQueries.queries()
                .indexQuery().create(null, "child_table").sql();

        assertTrue(sql.contains("CONSTRAINT_TYPE in ('PRIMARY KEY', 'UNIQUE')"));
    }

    @Test
    void keepsCrossSchemaForeignKeyTargetQualifiedButLeavesLocalTargetUnqualified() {
        assertEquals(RelationIdentity.table("tenant"), foreignKey("app", "app").reference());
        assertEquals(RelationIdentity.of(null, "identity", "tenant"), foreignKey("app", "identity").reference());
        assertEquals(ReferentialAction.CASCADE, foreignKey("app", "identity").onDelete());
        assertEquals(ReferentialAction.RESTRICT, foreignKey("app", "identity").onUpdate());
    }

    private static com.flying.orm.core.metadata.ForeignKeyDefinition foreignKey(
            String tableSchema,
            String referencedTableSchema) {
        RelationalTableDefinition metadata = FormMetadataRowConverter.toTableDefinition(RelationIdentity.table("orders"),
                FormMetadataRowConverter.tableBuilder(RelationIdentity.table("orders"), List.of(
                        Map.of("COLUMN_NAME", "tenant_id", "DATA_TYPE", "BIGINT")), value -> value),
                List.of(),
                List.of(Map.of("FOREIGN_KEY_NAME", "fk_orders_tenant",
                               "TABLE_SCHEMA", tableSchema,
                               "COLUMN_NAME", "tenant_id",
                               "REFERENCED_TABLE_SCHEMA", referencedTableSchema,
                               "REFERENCED_TABLE_NAME", "tenant",
                               "ON_DELETE", "CASCADE",
                               "ON_UPDATE", "RESTRICT",
                               "REFERENCED_COLUMN_NAME", "id")), List.of(), MetadataQueryProfile.SnapshotDialect.PARTIAL);
        return metadata.foreignKey("fk_orders_tenant");
    }
}
