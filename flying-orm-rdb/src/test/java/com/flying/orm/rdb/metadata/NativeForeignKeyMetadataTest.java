package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.TableMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NativeForeignKeyMetadataTest {

    @Test
    void nativeForeignKeyQueriesProjectSourceAndReferencedSchema() {
        List<InformationSchemaFormMetadataReader.Queries> readers = List.of(
                H2ReactiveFormMetadataReader.queries(),
                MySqlReactiveFormMetadataReader.queries(),
                PostgreSqlReactiveFormMetadataReader.queries(),
                OracleReactiveFormMetadataReader.queries(),
                SqlServerReactiveFormMetadataReader.queries());

        assertTrue(readers.stream().allMatch(queries -> {
            String sql = queries.foreignKeyQuery().create(null, "orders").sql();
            return sql.contains("TABLE_SCHEMA") && sql.contains("REFERENCED_TABLE_SCHEMA");
        }));
    }

    @Test
    void mysqlIndexQueryDoesNotHideAnIndexOnlyBecauseItsNameMatchesAForeignKey() {
        String sql = MySqlReactiveFormMetadataReader.queries()
                .indexQuery().create(null, "child_table").sql();

        assertTrue(sql.contains("CONSTRAINT_TYPE in ('PRIMARY KEY', 'UNIQUE')"));
    }

    @Test
    void keepsCrossSchemaForeignKeyTargetQualifiedButLeavesLocalTargetUnqualified() {
        assertEquals("tenant", foreignKey("app", "app").referenceTable());
        assertEquals("identity.tenant", foreignKey("app", "identity").referenceTable());
    }

    private static com.flying.orm.core.metadata.ForeignKeyMetadata foreignKey(
            String tableSchema,
            String referencedTableSchema) {
        DynamicForm form = DynamicForm.builder("orders", "orders")
                                      .addField(DynamicField.of("tenant_id", "BIGINT"))
                                      .build();
        TableMetadata metadata = FormMetadataRowConverter.toTableMetadata(
                "orders",
                form,
                List.of(),
                List.of(Map.of("FOREIGN_KEY_NAME", "fk_orders_tenant",
                               "TABLE_SCHEMA", tableSchema,
                               "COLUMN_NAME", "tenant_id",
                               "REFERENCED_TABLE_SCHEMA", referencedTableSchema,
                               "REFERENCED_TABLE_NAME", "tenant",
                               "REFERENCED_COLUMN_NAME", "id")));
        return metadata.foreignKey("fk_orders_tenant");
    }
}
