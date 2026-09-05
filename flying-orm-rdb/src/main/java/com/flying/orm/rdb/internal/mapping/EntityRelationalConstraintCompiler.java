package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.annotation.TableCheck;
import com.flying.orm.core.annotation.TableForeignKey;
import com.flying.orm.core.annotation.TableIndex;
import com.flying.orm.core.annotation.TableIndexColumn;
import com.flying.orm.core.annotation.TablePrimaryKey;
import com.flying.orm.core.annotation.TableUnique;
import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.rdb.internal.mapping.EntityRelationalMetadataCompiler.Property;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.schema.RelationalObjectNameGenerator;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;

/** 合并实体层次上的主键、索引、外键和检查约束；只在 descriptor 冷路径执行。 */
final class EntityRelationalConstraintCompiler {

    // Oracle 12c 是内建方言中最短的标识符上限。框架生成的名字固定满足这个下限，
    // 因而同一个实体描述可以安全交给任一受支持方言，显式命名仍由调用方完整控制。
    private static final RelationalObjectNameGenerator PORTABLE_NAMES =
            new RelationalObjectNameGenerator(30);

    private EntityRelationalConstraintCompiler() {
    }

    static PrimaryKeyDefinition primaryKey(
            Class<?> entityType,
            RelationIdentity relation,
            Map<String, Property> properties,
            PrimaryKeyDefinition declared) {
        List<Property> idProperties = properties.values().stream()
                .filter(property -> property.metadata().primaryKey())
                .toList();
        Set<String> idColumns = idProperties.stream()
                .map(property -> property.metadata().columnName())
                .collect(java.util.stream.Collectors.toCollection(HashSet::new));
        TablePrimaryKey annotation = entityType.getAnnotation(TablePrimaryKey.class);
        PrimaryKeyDefinition annotated = null;
        if (annotation != null) {
            List<Property> ordered = annotation.properties().length == 0
                    ? idProperties
                    : resolveProperties(properties, List.of(annotation.properties()), "primary key");
            Set<String> annotatedColumns = ordered.stream()
                    .map(property -> property.metadata().columnName())
                    .collect(java.util.stream.Collectors.toCollection(HashSet::new));
            if (!annotatedColumns.equals(idColumns) || ordered.size() != idProperties.size()) {
                throw new MappingException("@TablePrimaryKey properties must match all @TableId properties: "
                                                   + entityType.getName());
            }
            String name = defaultName(annotation.name(), generatedPrimaryKeyName(relation));
            annotated = new PrimaryKeyDefinition(name, columns(ordered));
        }
        if (declared != null && !sameColumns(idColumns, declared.columns())) {
            throw new MappingException("programmatic primary key must match all @TableId properties: "
                                               + entityType.getName());
        }
        if (annotated != null && declared != null && !annotated.equals(declared)) {
            throw new MappingException("annotated and programmatic primary key definitions differ: "
                                               + entityType.getName());
        }
        if (declared != null) {
            return declared;
        }
        if (annotated != null) {
            return annotated;
        }
        return idProperties.isEmpty()
                ? null
                : new PrimaryKeyDefinition(generatedPrimaryKeyName(relation), columns(idProperties));
    }

    static void mergeAnnotatedUniques(
            Class<?> entityType,
            Map<String, Property> properties,
            Map<String, UniqueConstraintDefinition> definitions) {
        for (TableUnique annotation : annotationsFromHierarchy(entityType, TableUnique.class)) {
            String id = requiredId(annotation.id(), "unique constraint id");
            List<Property> members = resolveProperties(
                    properties, List.of(annotation.properties()), "unique constraint");
            UniqueConstraintDefinition definition = new UniqueConstraintDefinition(
                    defaultName(annotation.name(), id), columns(members));
            merge(definitions, id, definition, Object::equals, "unique constraint");
        }
    }

    static void mergeAnnotatedIndexes(
            Class<?> entityType,
            Map<String, Property> properties,
            Map<String, IndexDefinition> definitions) {
        for (TableIndex annotation : annotationsFromHierarchy(entityType, TableIndex.class)) {
            String id = requiredId(annotation.id(), "index id");
            IndexDefinition.Builder builder = IndexDefinition.builder(
                    defaultName(annotation.name(), id)).unique(annotation.unique());
            Set<String> seen = new HashSet<>();
            for (TableIndexColumn column : annotation.columns()) {
                Property property = property(properties, column.property(), "index");
                String physical = property.metadata().columnName();
                if (!seen.add(physical)) {
                    throw new MappingException("index must not repeat an entity property");
                }
                builder.addKey(column.direction() == TableIndexColumn.Direction.ASC
                                       ? IndexKeyPart.asc(physical)
                                       : IndexKeyPart.desc(physical));
            }
            IndexDefinition definition = build("index", builder::build);
            merge(definitions, id, definition,
                  EntityRelationalConstraintCompiler::sameIndex, "index");
        }
    }

    static void mergeAnnotatedForeignKeys(
            Class<?> entityType,
            Map<String, Property> properties,
            Map<String, ForeignKeyDefinition> definitions) {
        for (TableForeignKey annotation : annotationsFromHierarchy(
                entityType, TableForeignKey.class)) {
            String id = requiredId(annotation.id(), "foreign key id");
            List<Property> local = resolveProperties(
                    properties, List.of(annotation.localProperties()), "foreign key");
            TargetProperties target = targetProperties(
                    annotation.targetEntity(), annotation.targetProperties());
            if (local.size() != target.columns().size()) {
                throw new MappingException("foreign key local and target property counts must match");
            }
            ForeignKeyDefinition.Builder builder = ForeignKeyDefinition.builder(
                    defaultName(annotation.name(), id));
            local.forEach(property -> builder.addColumn(property.metadata().columnName()));
            builder.reference(target.identity());
            target.columns().forEach(builder::addReferenceColumn);
            builder.onUpdate(annotation.onUpdate()).onDelete(annotation.onDelete());
            ForeignKeyDefinition definition = build("foreign key", builder::build);
            merge(definitions, id, definition,
                  EntityRelationalConstraintCompiler::sameForeignKey, "foreign key");
        }
    }

    static void mergeAnnotatedChecks(
            Class<?> entityType,
            Map<String, Property> properties,
            Map<String, CheckConstraintDefinition> definitions) {
        for (TableCheck annotation : annotationsFromHierarchy(entityType, TableCheck.class)) {
            String id = requiredId(annotation.id(), "check constraint id");
            Property property = property(properties, annotation.property(), "check constraint");
            CheckPredicate predicate = predicate(annotation, property);
            CheckConstraintDefinition definition = CheckConstraintDefinition.of(
                    defaultName(annotation.name(), id), predicate);
            merge(definitions, id, definition, Object::equals, "check constraint");
        }
    }

    static Set<String> singleColumnUniqueColumns(
            Iterable<UniqueConstraintDefinition> uniques,
            Iterable<IndexDefinition> indexes) {
        Set<String> columns = new HashSet<>();
        for (UniqueConstraintDefinition unique : uniques) {
            if (unique.columns().size() == 1) {
                columns.add(unique.columns().getFirst());
            }
        }
        for (IndexDefinition index : indexes) {
            if (index.unique() && index.keys().size() == 1) {
                columns.add(index.keys().getFirst().column());
            }
        }
        return Set.copyOf(columns);
    }

    private static TargetProperties targetProperties(
            Class<?> targetType,
            String[] propertyNames) {
        EntityCompilation<?> target = new EntityMetadataCompiler(EntityNamingStrategy.SNAKE_CASE)
                .compileModel(Objects.requireNonNull(
                        targetType, "foreign key target entity must not be null"));
        LinkedHashMap<String, EntityFieldMetadata> properties = new LinkedHashMap<>();
        for (int index = 0; index < target.persistentFields().size(); index++) {
            properties.put(target.persistentFields().get(index).getName(),
                           target.fieldMetadata().get(index));
        }
        List<String> columns = new ArrayList<>(propertyNames.length);
        Set<String> seen = new HashSet<>();
        for (String propertyName : propertyNames) {
            String safeName = requiredId(propertyName, "foreign key target property");
            EntityFieldMetadata metadata = properties.get(safeName);
            if (metadata == null) {
                throw new MappingException(
                        "foreign key references an unknown target entity property");
            }
            if (target.protections().encrypted(metadata.columnName()).isPresent()) {
                throw new MappingException(
                        "foreign key must not reference an encrypted target entity property");
            }
            if (!seen.add(metadata.columnName())) {
                throw new MappingException(
                        "foreign key must not repeat a target entity property");
            }
            columns.add(metadata.columnName());
        }
        return new TargetProperties(target.relationIdentity(), List.copyOf(columns));
    }

    private static CheckPredicate predicate(TableCheck annotation, Property property) {
        String column = property.metadata().columnName();
        String[] values = annotation.literalValues();
        return switch (annotation.operator()) {
            case EQUAL -> comparison(
                    column, CheckPredicate.ComparisonOperator.EQUAL, values, property);
            case NOT_EQUAL -> comparison(
                    column, CheckPredicate.ComparisonOperator.NOT_EQUAL, values, property);
            case LESS_THAN -> comparison(
                    column, CheckPredicate.ComparisonOperator.LESS_THAN, values, property);
            case LESS_THAN_OR_EQUAL -> comparison(
                    column, CheckPredicate.ComparisonOperator.LESS_THAN_OR_EQUAL, values, property);
            case GREATER_THAN -> comparison(
                    column, CheckPredicate.ComparisonOperator.GREATER_THAN, values, property);
            case GREATER_THAN_OR_EQUAL -> comparison(
                    column, CheckPredicate.ComparisonOperator.GREATER_THAN_OR_EQUAL, values, property);
            case BETWEEN -> {
                requireLiteralCount(values, 2, "BETWEEN");
                yield CheckPredicate.range(column,
                                           property.mapping().readLiteral(values[0]),
                                           property.mapping().readLiteral(values[1]));
            }
            case IN -> {
                if (values.length == 0) {
                    throw new MappingException("IN check requires at least one literal value");
                }
                yield CheckPredicate.in(column, java.util.Arrays.stream(values)
                        .map(property.mapping()::readLiteral)
                        .toList());
            }
            case IS_NULL -> {
                requireLiteralCount(values, 0, "IS_NULL");
                yield CheckPredicate.isNull(column);
            }
            case IS_NOT_NULL -> {
                requireLiteralCount(values, 0, "IS_NOT_NULL");
                yield CheckPredicate.isNotNull(column);
            }
        };
    }

    private static CheckPredicate comparison(
            String column,
            CheckPredicate.ComparisonOperator operator,
            String[] values,
            Property property) {
        requireLiteralCount(values, 1, operator.name());
        return CheckPredicate.compare(
                column, operator, property.mapping().readLiteral(values[0]));
    }

    private static void requireLiteralCount(String[] values, int expected, String operator) {
        if (values.length != expected) {
            throw new MappingException(
                    operator + " check requires exactly " + expected + " literal values");
        }
    }

    private static <A extends Annotation> List<A> annotationsFromHierarchy(
            Class<?> entityType,
            Class<A> annotationType) {
        List<A> annotations = new ArrayList<>();
        // 每层只读自己的声明，保持与字段编译相同的父到子顺序。
        for (Class<?> persistentType : EntityMetadataHierarchy.persistentTypes(entityType)) {
            java.util.Collections.addAll(
                    annotations, persistentType.getDeclaredAnnotationsByType(annotationType));
        }
        return annotations;
    }

    private static List<Property> resolveProperties(
            Map<String, Property> properties,
            List<String> names,
            String owner) {
        if (names.isEmpty()) {
            throw new MappingException(owner + " properties must not be empty");
        }
        List<Property> resolved = new ArrayList<>(names.size());
        Set<String> seen = new HashSet<>();
        for (String name : names) {
            Property property = property(properties, name, owner);
            if (!seen.add(property.metadata().columnName())) {
                throw new MappingException(owner + " must not repeat an entity property");
            }
            resolved.add(property);
        }
        return List.copyOf(resolved);
    }

    private static Property property(
            Map<String, Property> properties,
            String name,
            String owner) {
        Property property = properties.get(requiredId(name, owner + " property"));
        if (property == null) {
            throw new MappingException(owner + " references an unknown entity property");
        }
        return property;
    }

    private static List<String> columns(List<Property> properties) {
        return properties.stream().map(property -> property.metadata().columnName()).toList();
    }

    private static boolean sameColumns(Set<String> expected, List<String> actual) {
        return expected.size() == actual.size() && expected.equals(new HashSet<>(actual));
    }

    private static <T> void merge(
            Map<String, T> definitions,
            String id,
            T annotated,
            BiPredicate<T, T> same,
            String owner) {
        T declared = definitions.putIfAbsent(id, annotated);
        if (declared != null && !same.test(declared, annotated)) {
            throw new MappingException("annotated and programmatic " + owner
                                               + " definitions differ for the same stable id");
        }
    }

    private static boolean sameIndex(IndexDefinition first, IndexDefinition second) {
        return first.name().equals(second.name())
                && first.unique() == second.unique()
                && first.keys().equals(second.keys());
    }

    private static boolean sameForeignKey(
            ForeignKeyDefinition first,
            ForeignKeyDefinition second) {
        return first.name().equals(second.name())
                && first.columns().equals(second.columns())
                && first.reference().equals(second.reference())
                && first.referenceColumns().equals(second.referenceColumns())
                && first.onUpdate() == second.onUpdate()
                && first.onDelete() == second.onDelete();
    }

    private static String defaultName(String value, String fallback) {
        String text = EntityRelationalMetadataCompiler.text(value);
        return text == null ? fallback : text;
    }

    private static String generatedPrimaryKeyName(RelationIdentity relation) {
        return PORTABLE_NAMES.generate(
                RelationalObjectNameGenerator.Kind.PRIMARY_KEY, relation.table());
    }

    private static String requiredId(String value, String name) {
        String text = EntityRelationalMetadataCompiler.text(value);
        if (text == null) {
            throw new MappingException(name + " must not be blank");
        }
        return text;
    }

    private static <T> T build(String owner, java.util.function.Supplier<T> factory) {
        return EntityRelationalMetadataCompiler.build(owner, factory);
    }

    private record TargetProperties(RelationIdentity identity, List<String> columns) {
    }
}
