package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.protection.ProtectedFormLayout;
import com.flying.orm.rdb.protection.ProtectedRelationalSchemaProjector;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtectedSchemaIndexProjectionParityTest {

    @Test
    void encryptedIndexProjectionPreservesDescendingDirection() {
        IndexDefinition index = IndexDefinition.builder("idx_secret_desc").addKey(IndexKeyPart.desc("secret")).build();
        IndexDefinition projected = ProtectedSchemaTarget.resolve(exactForm(false), List.of(index), List.of()).indexes().getFirst();
        assertEquals(IndexKeyPart.Direction.DESC, projected.keys().getFirst().direction());
        assertTrue(!projected.columns().equals(index.columns()));
    }

    @Test
    void unencryptedFieldUniquenessIsIncludedInBothCreationPaths() {
        DynamicForm form = DynamicForm.builder("ordinary_unique", "ordinary_unique")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("code", "VARCHAR(80)").withUnique(true)).build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(
                com.flying.orm.rdb.dialect.RdbDialect.h2());

        SchemaMigrationPlan plan = renderer.createTablePlan(form, List.of());
        assertEquals(renderer.createTable(form).stream().map(com.flying.orm.core.sql.render.SqlRequest::sql).toList(),
                plan.sqlTexts());
        assertEquals(1, plan.target().indexes().size());
        assertTrue(plan.target().indexes().getFirst().unique());
        assertEquals(List.of("code"), plan.target().indexes().getFirst().columns());
    }

    @Test
    void explicitOrdinaryUniqueIndexReplacesItsAutomaticEquivalent() {
        DynamicForm form = DynamicForm.builder("ordinary_unique", "ordinary_unique")
                .addField(DynamicField.of("code", "VARCHAR(80)").withUnique(true)).build();
        IndexDefinition explicit = IndexDefinition.builder("uq_code_business").unique().addColumn("code").build();

        ProtectedSchemaTarget target = ProtectedSchemaTarget.resolve(form, List.of(explicit), List.of());

        assertEquals(1, target.indexes().size());
        assertEquals("uq_code_business", target.indexes().getFirst().name());
        assertTrue(target.indexes().getFirst().unique());
        IndexDefinition conflict = IndexDefinition.builder(form.toTableDefinition().indexes().getFirst().name())
                .addColumn("code").build();
        assertThrows(IllegalArgumentException.class,
                () -> ProtectedSchemaTarget.resolve(form, List.of(conflict), List.of()));
    }

    @Test
    void explicitUniqueIndexRequiresStableUniqueTokensInTheLogicalForm() {
        IndexDefinition explicit = IndexDefinition.builder("uq_secret_business")
                .unique().addColumn("secret").build();

        assertThrows(IllegalArgumentException.class,
                () -> ProtectedSchemaTarget.resolve(exactForm(false), List.of(explicit), List.of()));
    }

    @Test
    void relationalUniqueIndexRequiresStableUniqueTokensInTheLogicalForm() {
        assertThrows(IllegalArgumentException.class,
                () -> ProtectedRelationalSchemaProjector.project(exactForm(false), logicalTable(true, false)));
    }

    @Test
    void relationalUniqueConstraintRequiresStableUniqueTokensInTheLogicalForm() {
        RelationalTableDefinition table = RelationalTableDefinition.builder(
                        RelationIdentity.table("protected_index_parity"))
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("secret", "VARCHAR").build())
                .addUnique(UniqueConstraintDefinition.of("uq_secret_business", "secret"))
                .build();

        assertThrows(IllegalArgumentException.class,
                () -> ProtectedRelationalSchemaProjector.project(exactForm(false), table));
    }

    @Test
    void declaredUniqueIndexKeepsTheSameTokenAcrossEncryptionKeyRotation() {
        DynamicForm form = exactForm(true);
        IndexDefinition explicit = IndexDefinition.builder("uq_secret_business")
                .unique().addColumn("secret").build();
        ProtectedSchemaTarget target = ProtectedSchemaTarget.resolve(form, List.of(explicit), List.of());
        String hashColumn = target.indexes().getFirst().columns().getFirst();
        byte[] oldKey = new byte[32];
        byte[] newKey = new byte[32];
        newKey[0] = 1;
        try (ProtectedFieldRuntime original = ProtectedFieldRuntime.create(
                    ProtectedFieldKeyRing.single("v1", oldKey));
             ProtectedFieldRuntime rotating = ProtectedFieldRuntime.create(
                    ProtectedFieldKeyRing.builder().current("v2", newKey).readable("v1", oldKey)
                            .uniqueSearchKey(oldKey).build())) {
            byte[] first = (byte[]) original.writeOperation(form, ProtectedFormLayout.physical(form),
                    DataScope.none(), ValueCodecRegistry.standard())
                    .prepare(Map.of("secret", "same business value")).values().get(hashColumn);
            byte[] second = (byte[]) rotating.writeOperation(form, ProtectedFormLayout.physical(form),
                    DataScope.none(), ValueCodecRegistry.standard())
                    .prepare(Map.of("secret", "same business value")).values().get(hashColumn);

            assertArrayEquals(first, second);
        }
    }

    @Test
    void explicitExactIndexesUseTheSameHashColumnInBothSchemaPaths() {
        DynamicForm form = exactForm(false);
        IndexDefinition legacyIndex = IndexDefinition.builder("idx_secret_lookup")
                .addColumn("secret").build();
        ProtectedSchemaTarget legacy = ProtectedSchemaTarget.resolve(
                form, List.of(legacyIndex), List.of());
        RelationalTableDefinition relational = ProtectedRelationalSchemaProjector.project(
                form, logicalTable(false, false)).tables().getFirst();

        IndexDefinition projectedLegacy = legacy.indexes().stream()
                .filter(index -> index.name().equals("idx_secret_lookup"))
                .findFirst().orElseThrow();
        assertEquals(relational.indexes().getFirst().keys().getFirst().column(),
                projectedLegacy.columns().getFirst());
        assertTrue(projectedLegacy.columns().getFirst().startsWith("__fop_e_"));
    }

    @Test
    void explicitUniqueExactIndexReplacesTheEquivalentAutomaticIndex() {
        DynamicForm form = exactForm(true);
        IndexDefinition explicit = IndexDefinition.builder("uq_secret_business")
                .unique().addColumn("secret").build();

        ProtectedSchemaTarget target = ProtectedSchemaTarget.resolve(
                form, List.of(explicit), List.of());

        List<IndexDefinition> uniqueIndexes = target.indexes().stream()
                .filter(IndexDefinition::unique).toList();
        assertEquals(1, uniqueIndexes.size());
        assertEquals("uq_secret_business", uniqueIndexes.getFirst().name());
        assertTrue(uniqueIndexes.getFirst().columns().getFirst().startsWith("__fop_e_"));
    }

    @Test
    void fieldUniqueWithoutAnExplicitIndexRetainsOneAutomaticHashIndex() {
        ProtectedSchemaTarget target = ProtectedSchemaTarget.resolve(
                exactForm(true), List.of(), List.of());

        assertEquals(1, target.indexes().stream().filter(IndexDefinition::unique).count());
        assertTrue(target.indexes().getFirst().columns().getFirst().startsWith("__fop_e_"));
    }

    @Test
    void bothPathsRejectCompositeAndNoExactProtectedIndexes() {
        DynamicForm exact = exactForm(false);
        IndexDefinition composite = IndexDefinition.builder("idx_composite")
                .addColumn("id").addColumn("secret").build();
        assertThrows(IllegalArgumentException.class,
                () -> ProtectedSchemaTarget.resolve(exact, List.of(composite), List.of()));

        DynamicForm noExact = DynamicForm.relationalBuilder(
                        "protected-index-parity", RelationIdentity.table("protected_index_parity"))
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", EncryptedFieldDefinition.builder().searchModes().build())
                .build();
        IndexDefinition unsupported = IndexDefinition.builder("idx_no_exact")
                .addColumn("secret").build();
        assertThrows(IllegalArgumentException.class,
                () -> ProtectedSchemaTarget.resolve(noExact, List.of(unsupported), List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> ProtectedRelationalSchemaProjector.project(noExact, logicalTable(false, false)));
    }

    private static DynamicForm exactForm(boolean unique) {
        return DynamicForm.relationalBuilder(
                        "protected-index-parity", RelationIdentity.table("protected_index_parity"))
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR").withUnique(unique))
                .encrypted("secret", EncryptedFieldDefinition.builder().build())
                .build();
    }

    private static RelationalTableDefinition logicalTable(boolean unique, boolean composite) {
        RelationalTableDefinition.Builder table = RelationalTableDefinition.builder(
                        RelationIdentity.table("protected_index_parity"))
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("secret", "VARCHAR").build());
        IndexDefinition.Builder index = IndexDefinition.builder("idx_secret_lookup")
                .unique(unique);
        if (composite) {
            index.addKey(IndexKeyPart.asc("id"));
        }
        table.addIndex(index.addKey(IndexKeyPart.asc("secret")).build());
        return table.build();
    }
}
