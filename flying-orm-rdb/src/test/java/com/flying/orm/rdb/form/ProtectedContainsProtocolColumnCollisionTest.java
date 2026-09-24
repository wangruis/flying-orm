package com.flying.orm.rdb.form;

import com.flying.orm.core.annotation.EncryptedField;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.TablePrimaryKey;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorSort;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtectedContainsProtocolColumnCollisionTest {

    @Test
    void layoutKeepsOwnersAndChoosesDistinctProtocolNamesIncludingRepeatedCaseInsensitiveCollisions() {
        for (List<String> owners : List.of(List.of("id"), List.of("field_tag"), List.of("token_hash"),
                List.of("FIELD_TAG", "field_tag_1", "Token_Hash", "token_hash_1"))) {
            DynamicForm form = form(owners);
            var layout = assertDoesNotThrow(() -> ProtectedContainsLayout.resolve(form).orElseThrow());
            List<String> columns = layout.table().fields().stream().map(DynamicField::name).toList();
            assertEquals(owners, columns.subList(0, owners.size()));
            assertEquals(columns.size(), columns.stream().map(name -> name.toLowerCase(Locale.ROOT))
                    .distinct().count());
            List<String> protocol = columns.subList(owners.size(), columns.size());
            assertTrue(protocol.stream().allMatch(name -> name.length() < 30));
            assertEquals(protocol, layout.indexes().getFirst().columns().subList(0, 2));
            assertEquals(columns, layout.indexes().get(1).columns());
            assertEquals(owners, layout.foreignKeys().getFirst().columns());
            assertEquals(owners, layout.foreignKeys().getFirst().referenceColumns());
            assertEquals(columns, ProtectedContainsLayout.resolve(form).orElseThrow().table().fields()
                    .stream().map(DynamicField::name).toList());
            if (owners.equals(List.of("id"))) {
                assertEquals(List.of("field_tag", "token_hash"), protocol);
            }
        }
    }

    @Test
    void writesAndBothContainsReadShapesUseTheSchemaProtocolColumns() {
        List<String> owners = List.of("FIELD_TAG", "field_tag_1", "Token_Hash", "token_hash_1");
        DynamicForm form = form(owners);
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql())
                    .withProtectedFields(runtime);
            assertDoesNotThrow(() -> {
                var layout = ProtectedContainsLayout.resolve(form).orElseThrow();
                String tag = layout.table().fields().get(owners.size()).name();
                String token = layout.table().fields().get(owners.size() + 1).name();
                LinkedHashMap<String, Object> values = new LinkedHashMap<>();
                owners.forEach(owner -> values.put(owner, 1L));
                values.put("secret", "alphabet");
                var physical = renderer.protection().physicalForm(form);
                var operation = renderer.protection().writeOperation(form, physical, DataScope.none());
                var prepared = operation.prepare(values);
                var request = renderer.protection().insert(prepared);
                var work = operation.protectedWrite(values, request, null,
                        ProtectedWriteWork.Kind.INSERT, operation.insertOwner(prepared, request)).orElseThrow();
                assertEquals(owners, work.ownerFields());
                assertTrue(work.deleteSql().endsWith(" and \"" + tag + "\" = ?"), work.deleteSql());
                assertTrue(work.insertSql().contains(", \"" + tag + "\", \"" + token + "\")"), work.insertSql());
                var where = ConditionGroup.and().add(ProtectedConditions.contains("secret", "pha")).build();
                var query = renderer.protection().prepareContainsQuery(form, form, where, DataScope.none())
                        .orElseThrow();
                var requests = List.of(renderer.protection().containsCandidates(query, 10).getFirst(),
                        renderer.protection().contains.rows(query, List.of()),
                        renderer.protection().contains.rows(query,
                                CursorPageQuery.after(10, List.of(1L, 1L, 1L, 1L),
                                        CursorSort.asc(owners.getFirst()))));
                for (var candidate : requests) {
                    assertTrue(candidate.sql().contains("\"" + tag + "\" = ?"), candidate.sql());
                    assertTrue(candidate.sql().contains("\"" + token + "\" in ("), candidate.sql());
                }
            });
        }
    }

    @Test
    void entitySchemaRetainsSameNamedOwnerKeyTypesAndUsesDistinctProtocolColumns() {
        var descriptor = assertDoesNotThrow(() -> EntitySchemaDescriptor.builder(CollisionEntity.class).build());
        var layout = ProtectedContainsLayout.resolve(descriptor.form()).orElseThrow();
        var side = descriptor.schema().tables().get(1);
        assertEquals(layout.table().fields().stream().map(DynamicField::name).toList(),
                side.columns().stream().map(column -> column.name()).toList());
        for (String owner : List.of("field_tag", "token_hash")) {
            assertEquals(descriptor.table().findColumn(owner).orElseThrow().databaseType(),
                    side.findColumn(owner).orElseThrow().databaseType());
        }
        for (DynamicField field : layout.table().fields().subList(2, 4)) {
            assertEquals(field.databaseType(), side.findColumn(field.name()).orElseThrow().databaseType());
        }
    }

    private static DynamicForm form(List<String> owners) {
        var builder = DynamicForm.builder("contains-collision", "contains_collision");
        owners.forEach(owner -> builder.addField(DynamicField.primaryKey(owner, "BIGINT")));
        return builder.addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.CONTAINS).build()).build();
    }

    @TableName("contains_collision_entity")
    @TablePrimaryKey(name = "pk_contains_collision_entity", properties = {"tag", "token"})
    private static final class CollisionEntity {
        @TableField("field_tag")
        @TableId
        private Long tag;
        @TableField("token_hash")
        @TableId
        private Long token;
        @EncryptedField(search = EncryptedSearchMode.CONTAINS)
        private String secret;
    }
}
