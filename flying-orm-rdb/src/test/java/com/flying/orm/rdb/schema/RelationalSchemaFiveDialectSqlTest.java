package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.ReferentialAction;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.plan.SqlExecutionStatements;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RelationalSchemaFiveDialectSqlTest {

    @Test
    void rendersCompleteCreateTableFactsForEveryBuiltInDialect() {
        RelationalTableDefinition table = orders();
        SchemaOperation operation = SchemaOperation.of(
                SchemaOperation.Kind.CREATE_TABLE,
                table.identity(),
                table.identity().table(),
                null,
                table,
                SchemaOperation.Compatibility.REQUIRES_REVIEW);

        for (RdbDialect dialect : List.of(
                RdbDialect.h2(),
                RdbDialect.mysql(),
                RdbDialect.postgresql(),
                RdbDialect.oracle(),
                RdbDialect.sqlServer())) {
            var requests = RelationalSchemaSqlRenderer.create(dialect.schema()).render(operation);
            String sql = normalize(requests.stream().map(request -> request.sql()).toList());

            assertTrue(sql.contains("constraint pk_orders primary key (id)"), dialect.name());
            assertTrue(sql.contains("constraint uk_orders_code unique (code)"), dialect.name());
            assertTrue(sql.contains("constraint ck_orders_id check (id > 0)"), dialect.name());
            assertTrue(sql.contains("constraint fk_orders_parent foreign key (parent_id)"), dialect.name());
            assertTrue(sql.contains("references app.orders (id) on delete cascade"), dialect.name());
            assertTrue(sql.contains("idx_orders_created on app.orders (created_at desc)"), dialect.name());
            assertTrue(sql.contains("订单表"), dialect.name());
            assertTrue(sql.contains("业务编码"), dialect.name());
            assertTrue(requests.stream().allMatch(request -> request.parameters().isEmpty()), dialect.name());
        }
    }

    @Test
    void mysqlMatchesCurrentTimestampPrecisionToTheTemporalColumn() {
        RelationalTableDefinition table = RelationalTableDefinition.builder(RelationIdentity.table("events"))
                .addColumn(ColumnDefinition.builder("created_at", "TIMESTAMP")
                        .temporalPrecision(6)
                        .nullable(false)
                        .defaultValue(ColumnDefault.currentTimestamp())
                        .build())
                .build();
        SchemaOperation operation = SchemaOperation.of(
                SchemaOperation.Kind.CREATE_TABLE, table.identity(), table.identity().table(),
                null, table, SchemaOperation.Compatibility.REQUIRES_REVIEW);

        String sql = RelationalSchemaSqlRenderer.create(RdbDialect.mysql().schema())
                .render(operation).getFirst().sql().toLowerCase(Locale.ROOT);

        assertTrue(sql.contains("datetime(6) not null default current_timestamp(6)"));
    }

    @Test
    void oraclePlacesTheDefaultBeforeTheNullabilityConstraint() {
        RelationalTableDefinition table = RelationalTableDefinition.builder(RelationIdentity.table("events"))
                .addColumn(ColumnDefinition.builder("created_at", "TIMESTAMP")
                        .temporalPrecision(6)
                        .nullable(false)
                        .defaultValue(ColumnDefault.currentTimestamp())
                        .build())
                .build();
        SchemaOperation operation = SchemaOperation.of(
                SchemaOperation.Kind.CREATE_TABLE, table.identity(), table.identity().table(),
                null, table, SchemaOperation.Compatibility.REQUIRES_REVIEW);

        String sql = RelationalSchemaSqlRenderer.create(RdbDialect.oracle().schema())
                .render(operation).getFirst().sql().toLowerCase(Locale.ROOT);

        assertTrue(sql.contains("timestamp(6) default current_timestamp not null"));
    }

    @Test
    void sqlServerResolvesCommentsForAnUnqualifiedTableAtExecutionTime() {
        RelationalTableDefinition table = RelationalTableDefinition.builder(RelationIdentity.table("events"))
                .comment("事件表")
                .addColumn(ColumnDefinition.builder("id", "BIGINT")
                        .nullable(false)
                        .comment("事件编号")
                        .build())
                .build();
        SchemaOperation operation = SchemaOperation.of(
                SchemaOperation.Kind.CREATE_TABLE, table.identity(), table.identity().table(),
                null, table, SchemaOperation.Compatibility.REQUIRES_REVIEW);

        var requests = RelationalSchemaSqlRenderer.create(RdbDialect.sqlServer().schema())
                .render(operation);
        requests.forEach(request -> SqlExecutionStatements.canonical(request, RdbDialect.sqlServer().name()));
        List<String> sql = requests.stream().map(request -> request.sql()).toList();

        List<String> comments = sql.stream()
                .filter(statement -> statement.contains("sp_addextendedproperty"))
                .toList();
        assertEquals(2, comments.size());
        assertTrue(comments.stream().allMatch(statement -> statement.startsWith("exec sp_executesql ")
                && statement.contains("object_schema_name(object_id(N''events''))")));
    }

    @Test
    void rendersASameNamedCheckAndIndexAsIndependentObjectsForEveryBuiltInDialect() {
        RelationalTableDefinition table = RelationalTableDefinition.builder(RelationIdentity.table("accounts"))
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addCheck(CheckConstraintDefinition.of(
                        "ix_accounts_id",
                        CheckPredicate.compare(
                                "id", CheckPredicate.ComparisonOperator.GREATER_THAN, 0)))
                .addIndex(IndexDefinition.builder("ix_accounts_id")
                                  .addKey(IndexKeyPart.asc("id"))
                                  .build())
                .build();
        SchemaOperation operation = SchemaOperation.of(
                SchemaOperation.Kind.CREATE_TABLE, table.identity(), table.identity().table(),
                null, table, SchemaOperation.Compatibility.REQUIRES_REVIEW);

        for (RdbDialect dialect : List.of(
                RdbDialect.h2(),
                RdbDialect.mysql(),
                RdbDialect.postgresql(),
                RdbDialect.oracle(),
                RdbDialect.sqlServer())) {
            String sql = normalize(RelationalSchemaSqlRenderer.create(dialect.schema())
                                           .render(operation).stream()
                                           .map(request -> request.sql())
                                           .toList());

            assertTrue(sql.contains("constraint ix_accounts_id check (id > 0)"), dialect.name());
            assertTrue(sql.contains("index ix_accounts_id on accounts (id asc)"), dialect.name());
        }
    }

    @Test
    void createsASameNamedForeignKeyIndexBeforeAddingTheForeignKey() {
        RelationIdentity child = RelationIdentity.table("child");
        RelationalTableDefinition table = RelationalTableDefinition.builder(child)
                .addColumn(ColumnDefinition.builder("parent_id", "BIGINT").build())
                .addIndex(IndexDefinition.builder("fk_child_parent")
                                  .addKey(IndexKeyPart.asc("parent_id"))
                                  .build())
                .addForeignKey(ForeignKeyDefinition.builder("fk_child_parent")
                                       .addColumn("parent_id")
                                       .reference(RelationIdentity.table("parent"))
                                       .addReferenceColumn("id")
                                       .build())
                .build();
        SchemaOperation operation = SchemaOperation.of(
                SchemaOperation.Kind.CREATE_TABLE, child, child.table(),
                null, table, SchemaOperation.Compatibility.REQUIRES_REVIEW);

        List<String> requests = RelationalSchemaSqlRenderer.create(RdbDialect.mysql().schema())
                .render(operation).stream()
                .map(request -> normalize(List.of(request.sql())))
                .toList();

        assertEquals(3, requests.size());
        assertTrue(requests.get(0).startsWith("create table child"));
        assertFalse(requests.get(0).contains("foreign key"));
        assertTrue(requests.get(1).startsWith("create index fk_child_parent"));
        assertTrue(requests.get(2).startsWith("alter table child add constraint fk_child_parent foreign key"));
    }

    private static RelationalTableDefinition orders() {
        RelationIdentity orders = RelationIdentity.of(null, "app", "orders");
        return RelationalTableDefinition.builder(orders)
                .comment("订单表")
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("code", "VARCHAR")
                        .length(64).nullable(false).comment("业务编码").build())
                .addColumn(ColumnDefinition.builder("parent_id", "BIGINT").build())
                .addColumn(ColumnDefinition.builder("created_at", "TIMESTAMP").nullable(false).build())
                .primaryKey(PrimaryKeyDefinition.of("pk_orders", "id"))
                .addUnique(UniqueConstraintDefinition.of("uk_orders_code", "code"))
                .addIndex(IndexDefinition.builder("idx_orders_created")
                        .addKey(IndexKeyPart.desc("created_at")).build())
                .addForeignKey(ForeignKeyDefinition.builder("fk_orders_parent")
                        .addColumn("parent_id")
                        .reference(orders)
                        .addReferenceColumn("id")
                        .onDelete(ReferentialAction.CASCADE)
                        .build())
                .addCheck(CheckConstraintDefinition.of(
                        "ck_orders_id",
                        CheckPredicate.compare("id", CheckPredicate.ComparisonOperator.GREATER_THAN, 0)))
                .build();
    }

    private static String normalize(List<String> requests) {
        return String.join("; ", requests)
                .replace("`", "")
                .replace("\"", "")
                .replace("[", "")
                .replace("]", "")
                .replaceAll("\\s+", " ")
                .toLowerCase(Locale.ROOT);
    }
}
