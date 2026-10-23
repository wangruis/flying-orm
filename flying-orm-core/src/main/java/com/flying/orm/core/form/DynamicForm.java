package com.flying.orm.core.form;

import com.flying.orm.core.internal.Names;
import com.flying.orm.core.join.QueryRelation;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.FeatureRegistry;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.FieldProtectionRegistry;
import com.flying.orm.core.protection.MaskedFieldDefinition;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * DynamicForm 是动态表单的只读定义，负责把 Java 表单字段稳定映射到表结构元数据。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public final class DynamicForm {

    private final String id;

    private final RelationIdentity relationIdentity;

    /** 自动数据库对象命名的稳定输入，不参与关系解析或 SQL 标识符渲染。 */
    private final String namingSource;

    private final List<DynamicField> fields;
    private volatile List<String> fieldNames;
    private final List<DynamicField> runtimeDefaultFields;
    private final FeatureRegistry features;
    private final boolean hasFieldFeatures;

    private final Map<String, DynamicField> fieldsByName;

    private final LogicDeleteDefinition logicDelete;

    private final TenantDefinition tenant;

    private final FieldProtectionRegistry protections;
    private final Map<String, QueryRelation> relations;

    /** 非缓存表单不支付摘要成本；首次需要缓存键或公开指纹时再稳定发布。 */
    private volatile String structureFingerprint;

    private DynamicForm(String id,
                        RelationIdentity relationIdentity,
                        String namingSource,
                        List<DynamicField> fields,
                        LogicDeleteDefinition logicDelete,
                        TenantDefinition tenant,
                        FieldProtectionRegistry protections,
                        Map<String, QueryRelation> relations,
                        FeatureRegistry features) {
        this.id = Names.requireText(id, "dynamic form id");
        this.relationIdentity = Objects.requireNonNull(relationIdentity, "dynamic form relation identity must not be null");
        this.namingSource = namingSource;
        this.features = features;

        List<DynamicField> copiedFields = List.copyOf(fields);
        List<DynamicField> boundFields = null;
        Map<String, DynamicField> indexedFields = new LinkedHashMap<>(Names.mapCapacity(copiedFields.size()));
        List<DynamicField> defaults = null;
        boolean fieldFeatures = false;
        for (int index = 0; index < copiedFields.size(); index++) {
            DynamicField original = copiedFields.get(index);
            DynamicField field = original.withFeatures(original.features().withParent(features));
            fieldFeatures |= !field.features().features().isEmpty();
            if (field != original) {
                if (boundFields == null) boundFields = new ArrayList<>(copiedFields);
                boundFields.set(index, field);
            }
            DynamicField previous = indexedFields.putIfAbsent(field.normalizedName(), field);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate dynamic field name");
            }
            if (field.runtimeDefault() != null) {
                if (defaults == null) defaults = new ArrayList<>();
                defaults.add(field);
            }
        }
        this.fields = boundFields == null ? copiedFields : List.copyOf(boundFields);
        this.runtimeDefaultFields = defaults == null ? List.of() : List.copyOf(defaults);
        this.hasFieldFeatures = fieldFeatures;
        this.fieldsByName = Map.copyOf(indexedFields);
        this.logicDelete = logicDelete == null
                ? null : DynamicFormValidation.logicDelete(logicDelete, fieldsByName);
        this.tenant = tenant == null
                ? null : DynamicFormValidation.tenant(tenant, fieldsByName);
        this.protections = DynamicFormValidation.protections(
                protections, fieldsByName, this.tenant, this.logicDelete);
        this.relations = Map.copyOf(relations);
    }

    /**
     * 创建动态表单构建器。
     *
     * @param id    表单 ID
     * @param table 物理表名
     * @return 动态表单构建器
     */
    public static Builder builder(String id, String table) {
        return new Builder(id, RelationIdentity.parse(table)).namingSource(table);
    }

    /**
     * 创建保留 catalog、schema 和 table 分段身份的动态表单构建器。
     *
     * @param id       表单 ID
     * @param identity 物理关系身份
     * @return 动态表单构建器
     */
    public static Builder relationalBuilder(String id, RelationIdentity identity) {
        RelationIdentity safeIdentity = Objects.requireNonNull(
                identity, "dynamic form relation identity must not be null");
        return new Builder(id, safeIdentity);
    }

    /**
     * 返回表单 ID。
     *
     * @return 表单 ID
     */
    public String id() {
        return id;
    }

    /**
     * 返回物理表名。
     *
     * @return 物理表名
     */
    public String table() {
        return relationIdentity.qualifiedName();
    }

    /** 自动索引、检索侧表的持久命名输入；与表名的展示格式分离。 */
    public String namingSource() {
        return namingSource;
    }

    /**
     * 返回不会猜测点号层级的物理关系身份。
     *
     * @return catalog、schema、table 的只读分段身份
     */
    public RelationIdentity relationIdentity() {
        return relationIdentity;
    }

    /**
     * 返回只读字段列表。
     *
     * @return 只读字段列表
     */
    public List<DynamicField> fields() {
        return fields;
    }

    /** 按声明顺序返回只读字段名，首次读取后随表单复用，不重复生成投影列表。 */
    public List<String> fieldNames() {
        List<String> names = fieldNames;
        if (names == null) {
            names = List.copyOf(fields.stream().map(DynamicField::name).toList());
            fieldNames = names;
        }
        return names;
    }

    /** 已声明应用默认值的字段；普通表单不在每次写入时扫描全部字段。 */
    public List<DynamicField> runtimeDefaultFields() {
        return runtimeDefaultFields;
    }

    /** 表级扩展，可由注册表的 parent 继承 Schema/数据库的共享声明。 */
    public FeatureRegistry features() {
        return features;
    }

    /** 构建时确定是否存在字段扩展；未声明时普通条件继续使用标准计划缓存。 */
    public boolean hasFieldFeatures() {
        return hasFieldFeatures;
    }

    /**
     * 返回只描述 SQL 结构、不包含表单业务 ID、租户值或逻辑删除值的稳定 SHA-256 指纹。
     *
     * <p>指纹在首次读取时计算一次，字段顺序、物理表、列类型、可空性、主键和数据库生成策略变化都会得到
     * 新指纹；仅更换表单业务 ID 不会破坏等价 SQL 计划复用。该值可安全用于有界结构计划缓存键。</p>
     *
     * @return 64 个十六进制字符的结构指纹
     */
    public String structureFingerprint() {
        String fingerprint = structureFingerprint;
        if (fingerprint != null) {
            return fingerprint;
        }
        synchronized (this) {
            fingerprint = structureFingerprint;
            if (fingerprint == null) {
                fingerprint = DynamicFormStructure.fingerprint(
                        relationIdentity, namingSource, fields, logicDelete, tenant, protections);
                structureFingerprint = fingerprint;
            }
            return fingerprint;
        }
    }

    /**
     * 返回逻辑删除配置。没配置就说明这个表单的 delete 是物理删除。
     *
     * @return 逻辑删除配置
     */
    public Optional<LogicDeleteDefinition> logicDelete() {
        return Optional.ofNullable(logicDelete);
    }

    /**
     * 返回租户配置。空表示这张表不走租户隔离。
     *
     * @return 租户配置
     */
    public Optional<TenantDefinition> tenant() {
        return Optional.ofNullable(tenant);
    }

    /**
     * 返回租户处理方式。没有配置租户字段时就是 NONE。
     *
     * @return 租户处理方式
     */
    public TenantStrategy tenantStrategy() {
        return tenant == null ? TenantStrategy.NONE : tenant.strategy();
    }

    /** @return 只包含显式字段声明的不可变保护 registry */
    public FieldProtectionRegistry protections() {
        return protections;
    }

    /** 可复用的查询关系，键按字段命名规则规范化；不参与物理结构或外键生成。 */
    public Map<String, QueryRelation> relations() {
        return relations;
    }

    /**
     * 按规范化字段名查找动态字段。
     *
     * @param name 字段名
     * @return 匹配字段
     */
    public Optional<DynamicField> findField(String name) {
        return Optional.ofNullable(fieldsByName.get(Names.key(name, "field name")));
    }

    /**
     * 按规范化字段名获取动态字段，不存在时抛出确定性异常。
     *
     * @param name 字段名
     * @return 匹配字段
     */
    public DynamicField field(String name) {
        DynamicField field = fieldsByName.get(Names.key(name, "field name"));
        if (field == null) {
            throw new IllegalArgumentException("dynamic field does not exist");
        }
        return field;
    }

    /** 发布完整表定义；列声明直接共享，不经由轻量列元数据中转。 */
    public RelationalTableDefinition toTableDefinition() {
        return toTableDefinition(relationIdentity);
    }

    /** 按调用方已经解析的物理身份发布列定义，不重新拼接或解析命名空间。 */
    public RelationalTableDefinition toTableDefinition(RelationIdentity identity) {
        RelationalTableDefinition.Builder builder = RelationalTableDefinition.builder(identity);
        List<String> primaryKeys = new ArrayList<>();
        for (DynamicField field : fields) {
            builder.addColumn(field.column());
            if (field.primaryKey()) {
                primaryKeys.add(field.name());
            }
            if (field.unique()) {
                builder.addIndex(IndexDefinition.builder(DynamicFormStructure.uniqueIndexName(namingSource, field.name()))
                        .unique().addKey(IndexKeyPart.asc(field.name())).build());
            }
        }
        if (!primaryKeys.isEmpty()) {
            builder.primaryKey(PrimaryKeyDefinition.of("pk_" + identity.table(), primaryKeys.toArray(String[]::new)));
        }
        return builder.build();
    }

    /**
     * 计算当前表单到目标表单的结构变更。
     *
     * @param target 目标表单
     * @return 动态表单变更集
     */
    public DynamicFormChangeSet diffTo(DynamicForm target) {
        DynamicForm safeTarget = Objects.requireNonNull(target, "target form must not be null");
        if (!mapsToSameRelation(safeTarget)) {
            throw new IllegalArgumentException("cannot diff forms mapped to different tables");
        }

        List<DynamicField> added = new ArrayList<>();
        List<DynamicField> removed = new ArrayList<>();
        List<FieldChange> changed = new ArrayList<>();

        for (DynamicField targetField : safeTarget.fields) {
            DynamicField sourceField = fieldsByName.get(targetField.normalizedName());
            if (sourceField == null) {
                added.add(targetField);
            } else if (!sourceField.name().equals(targetField.name())) {
                throw new IllegalArgumentException(
                        "case-only physical field rename is not an in-place field change");
            } else if (!sourceField.column().equals(targetField.column())
                    || sourceField.primaryKey() != targetField.primaryKey()
                    || sourceField.unique() != targetField.unique()) {
                changed.add(new FieldChange(sourceField, targetField));
            }
        }

        for (DynamicField sourceField : fields) {
            if (!safeTarget.fieldsByName.containsKey(sourceField.normalizedName())) {
                removed.add(sourceField);
            }
        }

        return new DynamicFormChangeSet(this, safeTarget, added, removed, changed);
    }

    /**
     * 判断两个表单是否映射到同一个物理关系。
     */
    public boolean mapsToSameRelation(DynamicForm target) {
        DynamicForm safeTarget = Objects.requireNonNull(target, "target form must not be null");
        return relationIdentity.equals(safeTarget.relationIdentity);
    }

    /**
     * DynamicForm 构建器。
     *
     * @author wangr
     * @date 2026-07-21
     * @version v1.0
     */
    public static final class Builder {

        private final String id;

        private final RelationIdentity relationIdentity;

        private String namingSource;

        private final List<DynamicField> fields = new ArrayList<>();

        private LogicDeleteDefinition logicDelete;

        private TenantDefinition tenant;

        private final FieldProtectionRegistry.Builder protections = FieldProtectionRegistry.builder();
        private Map<String, QueryRelation> relations = Map.of();
        private FeatureRegistry features = FeatureRegistry.empty();

        private Builder(String id, RelationIdentity relationIdentity) {
            this.id = Names.requireText(id, "dynamic form id");
            this.relationIdentity = relationIdentity;
            this.namingSource = relationIdentity.table();
        }

        /** 复制物理布局时保留命名输入；变更该值意味着变更自动生成的数据库对象名称。 */
        public Builder namingSource(String source) {
            this.namingSource = Names.requireText(source, "physical naming source");
            return this;
        }

        /**
         * 添加动态字段。
         *
         * @param field 动态字段
         * @return 当前构建器
         */
        public Builder addField(DynamicField field) {
            fields.add(Objects.requireNonNull(field, "dynamic field must not be null"));
            return this;
        }

        /** 配置本表扩展，字段上的同 id 声明优先。 */
        public Builder features(FeatureRegistry features) {
            this.features = Objects.requireNonNull(features, "form features");
            return this;
        }

        /** 声明查询关系；目标在查询引用该关系时才解析。 */
        public Builder relation(String name, QueryRelation relation) {
            String key = Names.key(name, "query relation name");
            if (relations.isEmpty()) relations = new LinkedHashMap<>();
            if (relations.putIfAbsent(key, Objects.requireNonNull(relation, "query relation must not be null")) != null) {
                throw new IllegalArgumentException("duplicate query relation: " + name);
            }
            return this;
        }

        /**
         * 声明逻辑删除字段，默认 0 表示未删除、1 表示已删除。
         *
         * @param fieldName 删除标记字段名
         * @return 当前构建器
         */
        public Builder logicDelete(String fieldName) {
            return logicDelete(fieldName, 0, 1);
        }

        /**
         * 声明逻辑删除字段和值。这里不限制值类型，后面绑定 SQL 参数时会走统一值编解码。
         *
         * @param fieldName       删除标记字段名
         * @param notDeletedValue 未删除值
         * @param deletedValue    已删除值
         * @return 当前构建器
         */
        public Builder logicDelete(String fieldName, Object notDeletedValue, Object deletedValue) {
            this.logicDelete = LogicDeleteDefinition.of(fieldName, notDeletedValue, deletedValue);
            return this;
        }

        /**
         * 声明这个表单的租户字段和处理方式。
         *
         * <p>AUTO 用于由服务端上下文补齐租户值；MANUAL 用于调用方明确传值，再由后续写入链路校验。</p>
         *
         * @param fieldName 租户字段名
         * @param strategy  租户处理方式，只能是 AUTO 或 MANUAL
         * @return 当前构建器
         */
        public Builder tenant(String fieldName, TenantStrategy strategy) {
            this.tenant = TenantDefinition.of(fieldName, strategy);
            return this;
        }

        /** 声明字段加密和允许的保护搜索能力。 */
        public Builder encrypted(String fieldName, EncryptedFieldDefinition definition) {
            protections.encrypted(fieldName, definition);
            return this;
        }

        /** 声明字段的业务结果脱敏策略。 */
        public Builder masked(String fieldName, MaskedFieldDefinition definition) {
            protections.masked(fieldName, definition);
            return this;
        }

        /**
         * 构建动态表单定义。
         *
         * @return 动态表单定义
         */
        public DynamicForm build() {
            return new DynamicForm(
                    id, relationIdentity, namingSource, fields, logicDelete, tenant, protections.build(), relations, features);
        }
    }
}
