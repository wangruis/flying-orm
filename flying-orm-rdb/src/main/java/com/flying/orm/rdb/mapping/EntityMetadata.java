package com.flying.orm.rdb.mapping;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.join.QueryRelation;
import com.flying.orm.core.internal.hash.StableDigest;
import com.flying.orm.core.internal.hash.StableEncoder;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalMetadataFingerprint;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.FieldProtectionRegistry;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.mapping.EntityFieldNames;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 保存一个实体对应的表、字段、主键、版本字段和逻辑删除字段信息。
 * 元数据创建完成后不会再变化，可以被多个并发请求安全复用。
 *
 * @param <T> 实体类型
 * @author wangr
 * @date 2026-07-30
 * @version v1.0
 */
public final class EntityMetadata<T> {

    private static final StableDigest.Domain STRUCTURE_DOMAIN =
            StableDigest.domain("entity-physical-structure/v2");

    private final Class<T> type;

    private final String formId;

    private final RelationIdentity relationIdentity;

    private final List<EntityFieldMetadata> fields;
    private final List<EntityFieldMetadata> primaryKeyFields;
    private final Optional<EntityFieldMetadata> versionField;
    private final Optional<EntityFieldMetadata> logicDeleteField;
    private final boolean enumConditionValues;
    private final boolean customMappings;
    private final List<String> defaultProjection;
    private final List<PageSort> defaultSorts;

    private final Map<String, EntityFieldMetadata> fieldsByName;
    private final EntityFieldMetadata tenantField;
    private final TenantStrategy tenantStrategy;
    private final DynamicForm dynamicForm;
    private final List<IndexDefinition> targetIndexes;
    private final String structureFingerprint;

    EntityMetadata(Class<T> type,
                   String formId,
                   RelationIdentity relationIdentity,
                   String namingSource,
                   List<EntityFieldMetadata> fields,
                   String tenantField,
                   TenantStrategy tenantStrategy,
                   FieldProtectionRegistry protections,
                   Map<String, QueryRelation> relations) {
        this.type = Objects.requireNonNull(type, "entity type must not be null");
        this.formId = requireText(formId, "entity form id");
        this.relationIdentity = Objects.requireNonNull(relationIdentity, "relation identity must not be null");
        this.fields = List.copyOf(fields);
        this.primaryKeyFields = this.fields.stream().filter(EntityFieldMetadata::primaryKey).toList();
        this.versionField = this.fields.stream().filter(EntityFieldMetadata::version).findFirst();
        this.logicDeleteField = this.fields.stream().filter(EntityFieldMetadata::logicDelete).findFirst();
        this.enumConditionValues = this.fields.stream().anyMatch(field -> field.enumValueMember() != null
                || field.enumStorage() != EntityEnumStorage.NONE);
        this.customMappings = this.fields.stream().anyMatch(field -> field.customMapping() != null);
        this.defaultProjection = this.fields.stream().allMatch(EntityFieldMetadata::selectable)
                ? null : this.fields.stream().filter(EntityFieldMetadata::selectable)
                        .map(EntityFieldMetadata::columnName).toList();
        this.defaultSorts = this.fields.stream().filter(EntityFieldMetadata::ordered)
                .sorted(Comparator.comparingInt(EntityFieldMetadata::orderPriority))
                .map(field -> field.orderAscending()
                        ? PageSort.asc(field.columnName()) : PageSort.desc(field.columnName()))
                .collect(Collectors.toUnmodifiableList());
        this.fieldsByName = index(this.fields);
        this.tenantField = tenantField == null ? null : field(tenantField);
        this.tenantStrategy = this.tenantField == null
                ? TenantStrategy.NONE
                : Objects.requireNonNull(tenantStrategy, "tenant strategy must not be null");
        this.dynamicForm = buildDynamicForm(Objects.requireNonNull(
                protections, "field protection registry must not be null"), relations, namingSource);
        this.targetIndexes = dynamicForm.toTableDefinition().indexes();
        this.structureFingerprint = fingerprint(dynamicForm, targetIndexes);
    }

    /** 实体描述编译时将查询关系与字段放入同一个表单，物理结构不受关系声明影响。 */
    @InternalApi
    public static <T> EntityMetadata<T> create(
            Class<T> type, String formId, RelationIdentity relationIdentity, String namingSource,
            List<EntityFieldMetadata> fields, String tenantField, TenantStrategy tenantStrategy,
            FieldProtectionRegistry protections, Map<String, QueryRelation> relations) {
        return new EntityMetadata<>(
                type, formId, relationIdentity, namingSource,
                fields, tenantField, tenantStrategy, protections, relations);
    }

    /** @return 对应的 Java 实体类型 */
    public Class<T> type() {
        return type;
    }

    /** @return 由命名策略生成的动态表单标识 */
    public String formId() {
        return formId;
    }

    /** @return 实体对应的物理表名 */
    public String table() {
        return relationIdentity.table();
    }

    /** @return 按实体声明顺序保存的不可修改字段列表 */
    public List<EntityFieldMetadata> fields() {
        return fields;
    }

    /** Internal condition assembly can skip value-plan lookup for ordinary entity models. */
    @InternalApi
    public boolean hasEnumConditionValues() {
        return enumConditionValues;
    }

    boolean hasCustomMappings() {
        return customMappings;
    }

    /** null 表示所有字段均可默认查询；空列表表示没有可默认查询的字段，只在读取时拒绝。 */
    List<String> defaultProjection() {
        return defaultProjection;
    }

    List<PageSort> defaultSorts() {
        return defaultSorts;
    }

    /**
     * 按 Java 字段名或数据库列名查找字段，比较时忽略下划线、短横线和大小写。
     *
     * @param name Java 字段名或数据库列名
     * @return 对应字段元数据
     * @throws IllegalArgumentException 字段不存在时抛出
     */
    public EntityFieldMetadata field(String name) {
        EntityFieldMetadata field = findField(name).orElse(null);
        if (field == null) {
            throw new IllegalArgumentException("entity field does not exist");
        }
        return field;
    }

    /**
     * 尝试按 Java 字段名或数据库列名查找。映射计划用这个方法跳过 {@code @Transient} 属性，
     * 不会把“明确不持久化”误报成元数据损坏。
     */
    public Optional<EntityFieldMetadata> findField(String name) {
        return Optional.ofNullable(fieldsByName.get(EntityFieldNames.key(name)));
    }

    /** @return 第一个主键字段；实体没有主键注解时为空 */
    public Optional<EntityFieldMetadata> idField() {
        return primaryKeyFields.isEmpty() ? Optional.empty() : Optional.of(primaryKeyFields.getFirst());
    }

    /** @return 按实体字段顺序保存的全部主键；无主键时为空，复合键不能只取 idField() */
    public List<EntityFieldMetadata> primaryKeyFields() {
        return primaryKeyFields;
    }

    /** @return 乐观锁版本字段；没有声明时为空 */
    public Optional<EntityFieldMetadata> versionField() {
        return versionField;
    }

    /** @return 逻辑删除字段；没有声明时为空 */
    public Optional<EntityFieldMetadata> logicDeleteField() {
        return logicDeleteField;
    }

    /** @return 租户隔离字段；公共表或未声明租户约束时为空 */
    public Optional<EntityFieldMetadata> tenantField() {
        return Optional.ofNullable(tenantField);
    }

    /** @return 实体声明的租户值处理策略 */
    public TenantStrategy tenantStrategy() {
        return tenantStrategy;
    }

    /**
     * 返回实体声明生成的目标索引。
     *
     * @return 可并发复用的不可变目标索引集合
     */
    public List<IndexDefinition> targetIndexes() {
        return targetIndexes;
    }

    /**
     * 返回物理目标结构的稳定 SHA-256 指纹，用于安全复用同表的多个实体映射。
     *
     * <p>指纹覆盖完整关系身份、按规范列名稳定排序的完整列形状和按结构稳定排序的目标索引，不包含
     * Java 类名、表单 ID、字段声明顺序、上层框架策略、SQL 参数或凭据，使该指纹只回答
     * “物理结构是否相同”。</p>
     *
     * @return 小写十六进制结构指纹
     */
    public String structureFingerprint() {
        return structureFingerprint;
    }

    /** @return 供统一权重缓存使用的逻辑复杂度，不按对象字节数伪装精确内存测量 */
    int logicalWeight() {
        // 区域权重的计量单位是“反射字段槽”，不是对象个数再叠加字段数；固定对象头已由最小权重 1 表达。
        // 这样 N 字段实体的元数据与其 N 槽映射计划可在 2N 的明确预算内稳定共存。
        return Math.max(1, fields.size());
    }

    /**
     * 转成 Repository 和动态表单客户端共用的不可变表单定义。
     *
     * @return 包含字段、主键和逻辑删除定义的动态表单
     */
    public DynamicForm toDynamicForm() {
        return dynamicForm;
    }

    private DynamicForm buildDynamicForm(FieldProtectionRegistry protections, Map<String, QueryRelation> relations,
                                         String namingSource) {
        DynamicForm.Builder builder = DynamicForm.relationalBuilder(formId, relationIdentity).namingSource(namingSource);
        for (EntityFieldMetadata field : fields) {
            builder.addField(field.field());
        }
        logicDeleteField().ifPresent(field -> builder.logicDelete(field.columnName(),
                                                                   field.logicNotDeletedValue(),
                                                                   field.logicDeletedValue()));
        tenantField().ifPresent(field -> builder.tenant(field.columnName(), tenantStrategy));
        protections.encryptedFields().forEach(builder::encrypted);
        protections.maskedFields().forEach(builder::masked);
        relations.forEach(builder::relation);
        return builder.build();
    }

    private static String fingerprint(DynamicForm form, List<IndexDefinition> indexes) {
        StableEncoder descriptor = StableDigest.sha256(STRUCTURE_DOMAIN);
        RelationIdentity identity = form.relationIdentity();
        descriptor.marker("SEGMENTED_RELATION")
                      .nullableText("CATALOG", identity.catalog().orElse(null))
                      .nullableText("SCHEMA", identity.schema().orElse(null))
                      .text("TABLE", identity.table());
        form.fields().stream()
            .sorted(Comparator.comparing(field -> field.name().trim()))
            .forEach(field -> {
                descriptor.marker("FIELD")
                          .bool("FIELD_PRIMARY_KEY", field.primaryKey())
                          .bool("FIELD_UNIQUE", field.unique());
                RelationalMetadataFingerprint.encodeColumn(descriptor, field.column());
            });
        indexes.stream()
               .sorted(Comparator.comparing(index -> index.name().trim()))
               .forEach(index -> {
                   descriptor.marker("INDEX")
                             .text("INDEX_NAME", index.name().trim())
                             .bool("INDEX_UNIQUE", index.unique())
                             .integer("INDEX_COLUMN_COUNT", index.columns().size());
                   index.keys().forEach(key -> descriptor.text("INDEX_COLUMN", key.column().trim())
                           .text("INDEX_DIRECTION", key.direction().name()));
               });
        return descriptor.finishHex();
    }

    private static Map<String, EntityFieldMetadata> index(List<EntityFieldMetadata> fields) {
        Map<String, EntityFieldMetadata> values = new LinkedHashMap<>();
        for (EntityFieldMetadata field : fields) {
            putIndex(values, EntityFieldNames.key(field.name()), field);
            putIndex(values, EntityFieldNames.key(field.columnName()), field);
        }
        return Map.copyOf(values);
    }

    private static void putIndex(Map<String, EntityFieldMetadata> values,
                                 String normalizedName,
                                 EntityFieldMetadata field) {
        EntityFieldMetadata previous = values.putIfAbsent(normalizedName, field);
        if (previous != null && previous != field) {
            // 继承字段冲突若继续执行，条件和写入都可能命中错误列，必须在任何 SQL 生成前终止。
            throw new MappingException("entity fields map to the same name or column: "
                                               + previous.name() + " and " + field.name());
        }
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }
}
