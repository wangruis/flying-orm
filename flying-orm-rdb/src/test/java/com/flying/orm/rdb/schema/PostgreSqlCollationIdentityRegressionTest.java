package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgreSqlCollationIdentityRegressionTest {

    @Test
    void preservesCustomSchemaIdentityAndKeepsPgCatalogNamesUnqualified() {
        List<SqlRequest> requests = new ArrayList<>();
        SchemaSnapshot actual = read("tenant_a.custom_order", requests);
        String query = requests.stream().map(SqlRequest::sql)
                .filter(sql -> sql.contains("information_schema.columns c"))
                .findFirst().orElseThrow().toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");
        assertTrue(query.contains("when c.collation_schema = 'pg_catalog' then c.collation_name"));
        assertTrue(query.contains("else c.collation_schema || '.' || c.collation_name"));

        var dialect = RdbDialect.postgresql();
        assertTrue(SchemaDiffer.diff(table("tenant_a.custom_order"), actual,
                dialect.capabilities(), SchemaCompatibilityMode.EXACT, dialect.name(), dialect.schema())
                .operations().isEmpty());
        var changed = SchemaDiffer.diff(table("tenant_b.custom_order"), actual,
                dialect.capabilities(), SchemaCompatibilityMode.EXACT, dialect.name(), dialect.schema());
        assertEquals(List.of(SchemaOperation.Kind.CHANGE_COLUMN),
                changed.operations().stream().map(SchemaOperation::kind).toList());
        ColumnDefinition qualified = table("tenant_b.custom_order").columns().getFirst();
        SchemaOperation addColumn = SchemaOperation.of(SchemaOperation.Kind.ADD_COLUMN,
                RelationIdentity.table("orders"), qualified.name(), null, qualified,
                SchemaOperation.Compatibility.SAFE_INCREMENTAL);
        String ddl = RelationalSchemaSqlRenderer.create(dialect.schema()).render(addColumn).getFirst().sql();
        assertTrue(ddl.contains("collate \"tenant_b\".\"custom_order\""), ddl);

        assertTrue(SchemaDiffer.diff(table("default"), SchemaSnapshot.present(table("default")),
                dialect.capabilities(), SchemaCompatibilityMode.EXACT, dialect.name(), dialect.schema())
                .operations().isEmpty());
    }

    private static SchemaSnapshot read(String collation, List<SqlRequest> requests) {
        Map<String, Object> column = new LinkedHashMap<>();
        column.put("COLUMN_NAME", "name");
        column.put("DATA_TYPE", "varchar");
        column.put("PHYSICAL_DATA_TYPE", "character varying(64)");
        column.put("PHYSICAL_TYPE_SCHEMA", "pg_catalog");
        column.put("PHYSICAL_TYPE_NAME", "varchar");
        column.put("PHYSICAL_ARRAY", false);
        column.put("CHARACTER_MAXIMUM_LENGTH", 64);
        column.put("NULLABLE", true);
        column.put("COLUMN_REPRESENTABLE", true);
        column.put("COLUMN_COLLATION", collation);
        SyncSqlExecutor executor = new SyncSqlExecutor() {
            @Override
            public List<DynamicRow> query(SqlRequest request) {
                requests.add(request);
                if (request.sql().contains("information_schema.columns c")) {
                    return List.of(DynamicRow.copyOf(column));
                }
                return request.sql().contains("TABLE_COMMENT")
                        ? List.of(DynamicRow.copyOf(Map.of("TABLE_REPRESENTABLE", true))) : List.of();
            }

            @Override
            public long rowsUpdated(SqlRequest request) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
                throw new UnsupportedOperationException();
            }
        };
        return JdbcFormMetadataReaders.create(executor, RdbDialect.postgresql()).readSnapshot("orders");
    }

    private static RelationalTableDefinition table(String collation) {
        return RelationalTableDefinition.builder(RelationIdentity.table("orders"))
                .addColumn(ColumnDefinition.builder("name", "VARCHAR").length(64)
                        .collation(collation).build())
                .build();
    }
}
