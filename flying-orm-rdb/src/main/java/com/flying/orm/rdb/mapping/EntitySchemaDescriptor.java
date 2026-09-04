package com.flying.orm.rdb.mapping;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationalMetadataFingerprint;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.mapping.EntityRelationalMetadataCompiler;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 实体在 Repository 与 Schema 路径共享的完整关系描述。
 *
 * <p>描述对象只在显式进入关系元数据路径时创建。构建完成后，旧 CRUD 使用的
 * {@link EntityMetadata}/{@link DynamicForm} 与 Schema 使用的 {@link RelationalTableDefinition}
 * 来自同一次实体编译，不会各自猜测字段类型或重新扫描注解。</p>
 *
 * @param <T> 实体类型
 * @author wangr
 * @date 2026-09-03
 * @version v3.2
 */
public final class EntitySchemaDescriptor<T> {

    private final EntityTypeMappingRegistry typeMappings;
    private final EntityMetadata<T> metadata;
    private final DynamicForm form;
    private final RelationalTableDefinition table;
    private final ValueCodecRegistry valueCodecs;
    private final Map<String, EntityTypeMappingRegistry.Mapping> customFieldMappings;
    private final Map<DynamicField, EntityTypeMappingRegistry.Mapping> customFieldCodecs;
    private final String typeMappingsFingerprint;
    private final String relationalFingerprint;

    private EntitySchemaDescriptor(EntityTypeMappingRegistry typeMappings,
                                   EntityMetadata<T> metadata,
                                   RelationalTableDefinition table,
                                   Map<String, EntityTypeMappingRegistry.Mapping> fieldMappings) {
        this.typeMappings = Objects.requireNonNull(typeMappings, "entity type mappings must not be null");
        this.metadata = Objects.requireNonNull(metadata, "entity metadata must not be null");
        this.table = Objects.requireNonNull(table, "relational table must not be null");
        form = Objects.requireNonNull(metadata.toDynamicForm(), "entity dynamic form must not be null");
        if (form != metadata.toDynamicForm()) {
            throw new IllegalArgumentException("entity metadata must publish one stable dynamic form");
        }
        Map<String, EntityTypeMappingRegistry.Mapping> customMappings = new LinkedHashMap<>();
        Map<DynamicField, EntityTypeMappingRegistry.Mapping> fieldCodecs =
                new IdentityHashMap<>();
        Objects.requireNonNull(fieldMappings, "entity field mappings must not be null")
               .forEach((property, mapping) -> {
                   EntityTypeMappingRegistry.Mapping safeMapping = Objects.requireNonNull(
                           mapping, "entity field mapping must not be null");
                   if (!safeMapping.custom()) {
                       return;
                   }
                   EntityFieldMetadata field = metadata.field(property);
                   if (!safeMapping.databaseType().equals(field.databaseType())) {
                       throw new IllegalArgumentException(
                               "entity field mapping database type must match entity metadata");
                   }
                   customMappings.put(field.name(), safeMapping);
                   fieldCodecs.put(form.findField(field.columnName())
                                           .orElseThrow(() -> new IllegalArgumentException(
                                                   "entity field must exist in its dynamic form")), safeMapping);
               });
        customFieldMappings = customMappings.isEmpty() ? Map.of() : Map.copyOf(customMappings);
        customFieldCodecs = fieldCodecs.isEmpty()
                ? Map.of() : Collections.unmodifiableMap(new IdentityHashMap<>(fieldCodecs));
        valueCodecs = Objects.requireNonNull(typeMappings.valueCodecs(),
                                             "entity value codecs must not be null");
        typeMappingsFingerprint = Objects.requireNonNull(typeMappings.fingerprint(),
                                                         "entity type mappings fingerprint must not be null");
        relationalFingerprint = RelationalMetadataFingerprint.of(table);
    }

    /** 创建实体完整关系描述的构建器。 */
    public static <T> Builder<T> builder(Class<T> entityType) {
        return new Builder<>(entityType);
    }

    /**
     * 供内部统一编译器发布已经核对完成的三个视图。
     *
     * <p>公开构建入口仍然只有 {@link #builder(Class)}；该工厂只解决内部包之间无法使用包级构造器的问题。</p>
     */
    @InternalApi
    public static <T> EntitySchemaDescriptor<T> create(EntityTypeMappingRegistry typeMappings,
                                                        EntityMetadata<T> metadata,
                                                        RelationalTableDefinition table) {
        return new EntitySchemaDescriptor<>(typeMappings, metadata, table, Map.of());
    }

    /** 统一编译器发布字段级精确映射时使用；普通业务代码仍通过 builder 创建描述。 */
    @InternalApi
    public static <T> EntitySchemaDescriptor<T> create(
            EntityTypeMappingRegistry typeMappings,
            EntityMetadata<T> metadata,
            RelationalTableDefinition table,
            Map<String, EntityTypeMappingRegistry.Mapping> fieldMappings) {
        return new EntitySchemaDescriptor<>(typeMappings, metadata, table, fieldMappings);
    }

    /** @return 本描述使用的不可变类型映射注册表 */
    public EntityTypeMappingRegistry typeMappings() {
        return typeMappings;
    }

    /** @return Repository、读映射和写入计划共用的实体元数据 */
    public EntityMetadata<T> metadata() {
        return metadata;
    }

    /** @return 与 {@link #metadata()} 持有的同一个动态表单实例 */
    public DynamicForm form() {
        return form;
    }

    /** @return Schema、迁移和 DDL 共用的规范关系定义 */
    public RelationalTableDefinition table() {
        return table;
    }

    /** @return 类型映射注册表派生的只读值转换注册表 */
    public ValueCodecRegistry valueCodecs() {
        return valueCodecs;
    }

    /** @return 构建时计算的稳定类型映射指纹 */
    public String typeMappingsFingerprint() {
        return typeMappingsFingerprint;
    }

    /** @return 构建时计算的稳定关系结构指纹 */
    public String relationalFingerprint() {
        return relationalFingerprint;
    }

    Map<String, EntityTypeMappingRegistry.Mapping> customFieldMappings() {
        return customFieldMappings;
    }

    /** 启动装配器使用精确 DynamicField 身份挂接业务 codec，不在逐值路径重新解析类型。 */
    @InternalApi
    public Map<DynamicField, EntityTypeMappingRegistry.Mapping> customFieldCodecs() {
        return customFieldCodecs;
    }

    /**
     * 收集实体注解之外的显式关系定义。
     *
     * <p>构建器只在配置线程中使用。每次 {@link #build()} 都把当前集合复制成只读快照，之后继续配置
     * 同一个构建器不会反向修改已经发布的描述对象。</p>
     */
    public static final class Builder<T> {

        private final Class<T> entityType;
        private EntityTypeMappingRegistry typeMappings = EntityTypeMappingRegistry.standard();
        private PrimaryKeyDefinition primaryKey;
        private final Map<String, UniqueConstraintDefinition> uniqueConstraints = new LinkedHashMap<>();
        private final Map<String, IndexDefinition> indexes = new LinkedHashMap<>();
        private final Map<String, ForeignKeyDefinition> foreignKeys = new LinkedHashMap<>();
        private final Map<String, CheckConstraintDefinition> checks = new LinkedHashMap<>();

        private Builder(Class<T> entityType) {
            this.entityType = Objects.requireNonNull(entityType, "entity type must not be null");
        }

        /** 使用显式 Java 类型、数据库类型和 codec 注册表。 */
        public Builder<T> typeMappings(EntityTypeMappingRegistry typeMappings) {
            this.typeMappings = Objects.requireNonNull(typeMappings, "entity type mappings must not be null");
            return this;
        }

        /** 声明显式主键；重复声明立即失败，避免构建顺序决定最终结构。 */
        public Builder<T> primaryKey(PrimaryKeyDefinition definition) {
            if (primaryKey != null) {
                throw new IllegalStateException("entity primary key is already defined");
            }
            primaryKey = Objects.requireNonNull(definition, "entity primary key must not be null");
            return this;
        }

        /** 按稳定 ID 声明唯一约束。 */
        public Builder<T> unique(String id, UniqueConstraintDefinition definition) {
            addDefinition(uniqueConstraints, id, definition, "unique constraint");
            return this;
        }

        /** 按稳定 ID 声明索引。 */
        public Builder<T> index(String id, IndexDefinition definition) {
            addDefinition(indexes, id, definition, "index");
            return this;
        }

        /** 按稳定 ID 声明外键。 */
        public Builder<T> foreignKey(String id, ForeignKeyDefinition definition) {
            addDefinition(foreignKeys, id, definition, "foreign key");
            return this;
        }

        /** 按稳定 ID 声明受控 CHECK 约束。 */
        public Builder<T> check(String id, CheckConstraintDefinition definition) {
            addDefinition(checks, id, definition, "check constraint");
            return this;
        }

        /** 编译注解和显式定义，发布一份不可变的实体关系描述。 */
        public EntitySchemaDescriptor<T> build() {
            return EntityRelationalMetadataCompiler.compile(
                    entityType,
                    typeMappings,
                    primaryKey,
                    snapshot(uniqueConstraints),
                    snapshot(indexes),
                    snapshot(foreignKeys),
                    snapshot(checks));
        }

        private static <D> void addDefinition(Map<String, D> definitions,
                                              String id,
                                              D definition,
                                              String role) {
            String stableId = requireStableId(id, role);
            D safeDefinition = Objects.requireNonNull(definition, role + " must not be null");
            if (definitions.putIfAbsent(stableId, safeDefinition) != null) {
                throw new IllegalArgumentException(role + " stable id is already defined");
            }
        }

        private static String requireStableId(String id, String role) {
            if (id == null || id.isBlank()) {
                throw new IllegalArgumentException(role + " stable id must not be blank");
            }
            return id.trim();
        }

        private static <D> Map<String, D> snapshot(Map<String, D> definitions) {
            return Collections.unmodifiableMap(new LinkedHashMap<>(definitions));
        }
    }
}
