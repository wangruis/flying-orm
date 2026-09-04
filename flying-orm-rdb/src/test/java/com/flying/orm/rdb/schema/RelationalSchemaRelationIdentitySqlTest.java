package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class RelationalSchemaRelationIdentitySqlTest {

    @Test
    void preservesEveryRelationSegmentAcrossCompleteDdlPaths() {
        SchemaDialect dialect = SchemaDialect.builder()
                .quoteIdentifiers('"')
                .commentOnColumn()
                .commentOnTable()
                .build();
        RelationalSchemaSqlRenderer renderer = RelationalSchemaSqlRenderer.create(dialect);
        RelationIdentity relation = RelationIdentity.of("tenant.db", "audit.schema", "audit.logs");
        RelationIdentity reference = RelationIdentity.of("archive.db", "audit.schema", "source.logs");
        ColumnDefinition payload = ColumnDefinition.builder("payload", "VARCHAR")
                .length(64)
                .comment("payload comment")
                .build();
        ForeignKeyDefinition foreignKey = ForeignKeyDefinition.builder("fk_audit_source")
                .addColumn("source_id")
                .reference(reference)
                .addReferenceColumn("id")
                .build();
        IndexDefinition index = IndexDefinition.builder("idx_audit_payload")
                .addKey(IndexKeyPart.asc("payload"))
                .build();
        RelationalTableDefinition table = RelationalTableDefinition.builder(relation)
                .comment("audit table")
                .addColumn(ColumnDefinition.builder("source_id", "BIGINT").build())
                .addColumn(payload)
                .addForeignKey(foreignKey)
                .addIndex(index)
                .build();

        List<String> sql = new ArrayList<>();
        sql.addAll(render(renderer, operation(SchemaOperation.Kind.CREATE_TABLE,
                                             relation, relation.table(), null, table)));
        sql.addAll(render(renderer, operation(SchemaOperation.Kind.ADD_COLUMN,
                                             relation, payload.name(), null, payload)));
        sql.addAll(render(renderer, operation(SchemaOperation.Kind.ADD_FOREIGN_KEY,
                                             relation, foreignKey.name(), null, foreignKey)));
        sql.addAll(render(renderer, operation(SchemaOperation.Kind.ADD_INDEX,
                                             relation, index.name(), null, index)));
        sql.addAll(render(renderer, operation(SchemaOperation.Kind.DROP_INDEX,
                                             relation, index.name(), index, null)));

        String relationSql = "\"tenant.db\".\"audit.schema\".\"audit.logs\"";
        String referenceSql = "\"archive.db\".\"audit.schema\".\"source.logs\"";
        assertContains(sql, "create table " + relationSql);
        assertContains(sql, "alter table " + relationSql + " add column");
        assertContains(sql, "alter table " + relationSql + " add constraint");
        assertContains(sql, "references " + referenceSql);
        assertContains(sql, "index \"idx_audit_payload\" on " + relationSql);
        assertContains(sql, "comment on table " + relationSql);
        assertContains(sql, "comment on column " + relationSql + ".\"payload\"");
        assertContains(sql, "drop index \"tenant.db\".\"audit.schema\".\"idx_audit_payload\"");
    }

    private static SchemaOperation operation(SchemaOperation.Kind kind,
                                             RelationIdentity relation,
                                             String objectName,
                                             Object actual,
                                             Object desired) {
        return SchemaOperation.of(kind, relation, objectName, actual, desired,
                                  kind == SchemaOperation.Kind.DROP_INDEX
                                          ? SchemaOperation.Compatibility.COMPATIBLE_EXTRA
                                          : SchemaOperation.Compatibility.REQUIRES_REVIEW);
    }

    private static List<String> render(RelationalSchemaSqlRenderer renderer, SchemaOperation operation) {
        return renderer.render(operation).stream().map(request -> request.sql()).toList();
    }

    private static void assertContains(List<String> sql, String expected) {
        assertTrue(sql.stream().anyMatch(statement -> statement.contains(expected)),
                   () -> "expected SQL fragment <" + expected + "> in " + sql);
    }
}
