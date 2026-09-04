package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.RelationalFormDefinition;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RelationalFormCrudIdentityTest {

    @Test
    void crudRendersEveryRelationIdentitySegmentIndependently() {
        RelationalFormDefinition definition = RelationalFormDefinition.builder(
                        "orders", RelationIdentity.of("tenant.db", "sales.data", "order.items"))
                .addField(
                        DynamicField.of("code", "VARCHAR"),
                        ColumnDefinition.builder("code", "VARCHAR").build())
                .build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());

        String target = "\"tenant.db\".\"sales.data\".\"order.items\"";
        ConditionGroup byCode = ConditionGroup.and().where("code", "=", "A-1").build();

        assertTrue(renderer.insert(definition.form(), Map.of("code", "A-1")).sql()
                .startsWith("insert into " + target + " "));
        assertTrue(renderer.insertBatch(
                definition.form(), List.of(Map.of("code", "A-1"))).sql()
                .startsWith("insert into " + target + " "));
        assertTrue(renderer.select(definition.form(), byCode).sql().contains(" from " + target));
        assertTrue(renderer.count(definition.form(), byCode).sql().contains(" from " + target));
        assertTrue(renderer.update(
                definition.form(), Map.of("code", "A-2"), byCode).sql()
                .startsWith("update " + target + " "));
        assertTrue(renderer.delete(definition.form(), byCode).sql()
                .startsWith("delete from " + target + " "));
    }

    @Test
    void legacyDottedTableKeepsItsQualifiedNameSemantics() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());

        String sql = renderer.insert(
                DynamicForm.builder("orders", "sales.orders")
                        .addField(DynamicField.of("code", "VARCHAR"))
                        .build(),
                Map.of("code", "A-1")).sql();

        assertEquals("insert into \"sales\".\"orders\" (\"code\") values (?)", sql);
    }

    @Test
    void protectedContainsSideTableStaysInTheSegmentedNamespace() {
        DynamicForm form = DynamicForm.relationalBuilder(
                        "orders", RelationIdentity.of("tenant.db", "sales.data", "order.items"))
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.CONTAINS)
                        .build())
                .build();
        ConditionGroup where = ConditionGroup.and()
                .add(ProtectedConditions.contains("secret", "pha"))
                .build();

        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql())
                    .withProtectedFields(runtime);
            var query = renderer.protection()
                    .prepareContainsQuery(form, form, where, DataScope.none())
                    .orElseThrow();

            String sql = renderer.protection().containsCandidates(query, 10).getFirst().sql();

            assertTrue(sql.contains(" from \"tenant.db\".\"sales.data\".\"__fop_c_"), sql);
        }
    }
}
