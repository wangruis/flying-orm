package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MySqlForeignKeySupportIndexDiffTest {

    private static final RelationIdentity CHILD = RelationIdentity.table("child");

    @Test
    void exactDiffIgnoresOnlyTheMySqlIndexAutomaticallySupportingADesiredForeignKey() {
        RelationalTableDefinition desired = table(true, null);
        RelationalTableDefinition actual = table(true, supportIndex("fk_child_parent", "parent_id"));

        SchemaCompatibilityReport mysql = diff(desired, actual, RdbDialect.mysql());
        SchemaCompatibilityReport mysqlInstance = new SchemaDiffer().compare(
                desired, SchemaSnapshot.present(actual), RdbDialect.mysql().capabilities(),
                SchemaCompatibilityMode.EXACT);
        SchemaCompatibilityReport postgresql = diff(desired, actual, RdbDialect.postgresql());

        assertTrue(mysql.compatible());
        assertTrue(mysql.operations().isEmpty());
        assertTrue(mysqlInstance.compatible());
        assertTrue(mysqlInstance.operations().isEmpty());
        assertFalse(postgresql.compatible());
        assertEquals(List.of(SchemaOperation.Kind.DROP_INDEX),
                     postgresql.operations().stream().map(SchemaOperation::kind).toList());
    }

    @Test
    void explicitDesiredIndexStillUsesTheOrdinaryIndexComparison() {
        IndexDefinition index = supportIndex("fk_child_parent", "parent_id");
        RelationalTableDefinition desired = table(false, index);
        RelationalTableDefinition actual = table(true, index);

        SchemaCompatibilityReport report = diff(desired, actual, RdbDialect.mysql());

        assertEquals(List.of(SchemaOperation.Kind.DROP_FOREIGN_KEY),
                     report.operations().stream().map(SchemaOperation::kind).toList());
    }

    @Test
    void doesNotHideAnIndexWhoseShapeCannotBeAMySqlForeignKeySupportIndex() {
        RelationalTableDefinition desired = table(true, null);
        IndexDefinition different = IndexDefinition.builder("fk_child_parent")
                .addKey(IndexKeyPart.desc("parent_id"))
                .build();

        SchemaCompatibilityReport report = diff(desired, table(true, different), RdbDialect.mysql());

        assertEquals(List.of(SchemaOperation.Kind.DROP_INDEX),
                     report.operations().stream().map(SchemaOperation::kind).toList());
    }

    private static SchemaCompatibilityReport diff(RelationalTableDefinition desired,
                                                  RelationalTableDefinition actual,
                                                  RdbDialect dialect) {
        return SchemaDiffer.diff(desired, SchemaSnapshot.present(actual), dialect.capabilities(),
                                 SchemaCompatibilityMode.EXACT);
    }

    private static RelationalTableDefinition table(boolean foreignKey, IndexDefinition index) {
        RelationalTableDefinition.Builder builder = RelationalTableDefinition.builder(CHILD)
                .addColumn(ColumnDefinition.builder("parent_id", "BIGINT").build());
        if (index != null) {
            builder.addIndex(index);
        }
        if (foreignKey) {
            builder.addForeignKey(ForeignKeyDefinition.builder("fk_child_parent")
                                          .addColumn("parent_id")
                                          .reference(RelationIdentity.table("parent"))
                                          .addReferenceColumn("id")
                                          .build());
        }
        return builder.build();
    }

    private static IndexDefinition supportIndex(String name, String column) {
        return IndexDefinition.builder(name).addKey(IndexKeyPart.asc(column)).build();
    }
}
