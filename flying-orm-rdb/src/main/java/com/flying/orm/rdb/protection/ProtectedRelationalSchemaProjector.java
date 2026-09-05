package com.flying.orm.rdb.protection;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.ReferentialAction;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalSchemaDefinition;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.internal.InternalApi;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 把实体逻辑关系一次性投影成 CRUD 真正使用的最终物理关系。
 *
 * <p>这个投影只运行在 descriptor/Schema 冷路径。普通实体直接复用原表对象，不为未启用的保护能力
 * 增加复制、扫描或新对象。</p>
 *
 * @author wangr
 * @version v3.3
 */
@InternalApi
public final class ProtectedRelationalSchemaProjector {

    private static final DatabaseType CIPHERTEXT = DatabaseType.of("PROTECTED_BINARY");
    private static final DatabaseType HASH = DatabaseType.of("PROTECTED_HASH");

    private ProtectedRelationalSchemaProjector() {
    }

    /** 返回主表在首位、可选 CONTAINS 辅助表在后的最终物理 Schema。 */
    public static RelationalSchemaDefinition project(
            DynamicForm logicalForm,
            RelationalTableDefinition logicalTable) {
        DynamicForm form = Objects.requireNonNull(logicalForm, "logical form must not be null");
        RelationalTableDefinition table = Objects.requireNonNull(
                logicalTable, "logical relational table must not be null");
        if (form.protections().encryptedFields().isEmpty()) {
            return RelationalSchemaDefinition.of(List.of(table));
        }

        // Reuse the established protected-column validation and persisted naming contract.
        DynamicForm physicalForm = ProtectedFormLayout.physical(form);
        RelationalTableDefinition owner = projectOwner(form, table, physicalForm);
        RelationalSchemaDefinition.Builder schema = RelationalSchemaDefinition.builder()
                .addTable(owner);
        ProtectedContainsLayout.resolve(form)
                .map(layout -> projectContains(layout, owner))
                .ifPresent(schema::addTable);
        return schema.build();
    }

    private static RelationalTableDefinition projectOwner(
            DynamicForm form,
            RelationalTableDefinition logical,
            DynamicForm physicalForm) {
        Map<String, ColumnDefinition> logicalColumns = new LinkedHashMap<>();
        logical.columns().forEach(column -> logicalColumns.put(column.name(), column));

        RelationalTableDefinition.Builder owner = RelationalTableDefinition.builder(logical.identity())
                .comment(logical.comment());
        for (DynamicField field : physicalForm.fields()) {
            ColumnDefinition logicalColumn = logicalColumns.get(field.name());
            if (logicalColumn != null && form.protections().encrypted(field.name()).isEmpty()) {
                owner.addColumn(logicalColumn);
            } else if (logicalColumn != null) {
                requireNoRelationalTransform(logicalColumn);
                owner.addColumn(ColumnDefinition.builder(logicalColumn.identity(), CIPHERTEXT)
                        .nullable(logicalColumn.nullable())
                        .comment(logicalColumn.comment())
                        .build());
            } else {
                owner.addColumn(ColumnDefinition.builder(field.identity(), HASH)
                        .nullable(field.nullable())
                        .build());
            }
        }

        logical.primaryKey().ifPresent(primaryKey -> {
            requireNoProtectedColumn(form, primaryKey.columns(), "primary key");
            owner.primaryKey(primaryKey);
        });
        logical.uniqueConstraints().forEach(unique -> owner.addUnique(projectUnique(form, unique)));
        logical.indexes().forEach(index -> owner.addIndex(projectIndex(form, index)));
        logical.foreignKeys().forEach(foreignKey -> {
            requireNoProtectedColumn(form, foreignKey.columns(), "foreign key");
            owner.addForeignKey(foreignKey);
        });
        logical.checks().forEach(check -> {
            if (usesProtectedColumn(form, check.predicate())) {
                throw new IllegalArgumentException("check constraint must not reference an encrypted field");
            }
            owner.addCheck(check);
        });
        logical.partition().ifPresent(partition -> {
            if (protectedColumn(form, partition.column())) {
                throw new IllegalArgumentException(
                        "table partition must not reference an encrypted field");
            }
            owner.partition(partition);
        });
        return owner.build();
    }

    private static UniqueConstraintDefinition projectUnique(
            DynamicForm form,
            UniqueConstraintDefinition unique) {
        List<String> projected = ProtectedIndexProjection.columns(form, unique.columns(), true);
        if (projected.equals(unique.columns())) {
            return unique;
        }
        return UniqueConstraintDefinition.of(unique.name(), projected.getFirst());
    }

    private static IndexDefinition projectIndex(DynamicForm form, IndexDefinition index) {
        List<String> columns = index.keys().stream().map(IndexKeyPart::column).toList();
        List<String> projected = ProtectedIndexProjection.columns(form, columns, index.unique());
        if (projected.equals(columns)) {
            return index;
        }
        IndexKeyPart source = index.keys().getFirst();
        return IndexDefinition.builder(index.name())
                .unique(index.unique())
                .addKey(new IndexKeyPart(
                        projected.getFirst(), source.direction()))
                .build();
    }

    private static RelationalTableDefinition projectContains(
            ProtectedContainsLayout layout,
            RelationalTableDefinition owner) {
        DynamicForm sideForm = layout.table();
        RelationIdentity identity = sideForm.relationIdentity()
                .orElseGet(() -> RelationIdentity.table(sideForm.table()));
        Map<String, ColumnDefinition> ownerKeys = new LinkedHashMap<>();
        owner.primaryKey().orElseThrow().columns().forEach(name -> {
            ColumnDefinition column = owner.column(name);
            ownerKeys.put(column.identity().key(), column);
        });
        RelationalTableDefinition.Builder side = RelationalTableDefinition.builder(identity);
        for (DynamicField field : sideForm.fields()) {
            ColumnDefinition ownerColumn = ownerKeys.get(field.identity().key());
            if (ownerColumn == null) {
                side.addColumn(ColumnDefinition.builder(field.identity(), field.databaseType())
                        .nullable(field.nullable())
                        .length(field.length())
                        .precision(field.databaseType().isTemporal() ? null : field.precision())
                        .scale(field.scale())
                        .temporalPrecision(field.databaseType().isTemporal() ? field.precision() : null)
                        .build());
            } else {
                side.addColumn(copyOwnerKey(ownerColumn));
            }
        }
        layout.indexes().forEach(index -> side.addIndex(index(index)));
        layout.foreignKeys().forEach(foreignKey -> {
            ForeignKeyDefinition.Builder builder = ForeignKeyDefinition.builder(foreignKey.name())
                    .reference(owner.identity())
                    .onDelete(ReferentialAction.CASCADE);
            foreignKey.columns().forEach(builder::addColumn);
            foreignKey.referenceColumns().forEach(builder::addReferenceColumn);
            side.addForeignKey(builder.build());
        });
        return side.build();
    }

    private static ColumnDefinition copyOwnerKey(ColumnDefinition source) {
        return ColumnDefinition.builder(source.identity(), source.databaseType())
                .codecId(source.codecId())
                .nullable(false)
                .length(source.length())
                .precision(source.precision())
                .scale(source.scale())
                .temporalPrecision(source.temporalPrecision())
                .charset(source.charset())
                .collation(source.collation())
                .defaultValue(ColumnDefault.none())
                .generation(ValueGeneration.none())
                .build();
    }

    private static IndexDefinition index(IndexMetadata source) {
        IndexDefinition.Builder builder = IndexDefinition.builder(source.name())
                .unique(source.unique());
        source.columns().forEach(column -> builder.addKey(IndexKeyPart.asc(column)));
        return builder.build();
    }

    private static void requireNoRelationalTransform(ColumnDefinition column) {
        if (column.defaultValue().kind() != ColumnDefault.Kind.NONE) {
            throw new IllegalArgumentException("encrypted field must not declare a database default");
        }
        if (column.generation().generated()) {
            throw new IllegalArgumentException("encrypted field must not be database-generated");
        }
    }

    private static void requireNoProtectedColumn(
            DynamicForm form,
            List<String> columns,
            String owner) {
        if (!protectedColumns(form, columns).isEmpty()) {
            throw new IllegalArgumentException(owner + " must not reference an encrypted field");
        }
    }

    private static List<String> protectedColumns(DynamicForm form, List<String> columns) {
        List<String> protectedColumns = new ArrayList<>();
        for (String column : columns) {
            if (form.protections().encrypted(column).isPresent()) {
                protectedColumns.add(column);
            }
        }
        return protectedColumns;
    }

    private static boolean usesProtectedColumn(DynamicForm form, CheckPredicate predicate) {
        return switch (predicate) {
            case CheckPredicate.Comparison value -> protectedColumn(form, value.column());
            case CheckPredicate.Range value -> protectedColumn(form, value.column());
            case CheckPredicate.In value -> protectedColumn(form, value.column());
            case CheckPredicate.NullCheck value -> protectedColumn(form, value.column());
            case CheckPredicate.Logical value -> value.predicates().stream()
                    .anyMatch(child -> usesProtectedColumn(form, child));
            case CheckPredicate.Negation value -> usesProtectedColumn(form, value.predicate());
        };
    }

    private static boolean protectedColumn(DynamicForm form, String column) {
        return form.protections().encrypted(column).isPresent();
    }
}
