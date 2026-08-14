package com.flying.orm.core.form;

import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.FieldProtectionRegistry;
import com.flying.orm.core.protection.MaskedFieldDefinition;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
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

    private final String table;

    private final String normalizedTable;

    private final List<DynamicField> fields;

    private final Map<String, DynamicField> fieldsByName;

    private final LogicDeleteDefinition logicDelete;

    private final TenantDefinition tenant;

    private final FieldProtectionRegistry protections;

    /** 构造时一次计算，热路径只读取，不重复遍历字段或执行摘要算法。 */
    private final String structureFingerprint;

    private DynamicForm(String id,
                        String table,
                        List<DynamicField> fields,
                        LogicDeleteDefinition logicDelete,
                        TenantDefinition tenant,
                        FieldProtectionRegistry protections) {
        this.id = FormNames.requireText(id, "dynamic form id");
        this.table = FormNames.requireText(table, "dynamic form table");
        // 物理表名最终会进入方言标识符渲染。大小写可能代表两个不同的 quoted identifier，不能像字段查找键一样折叠。
        this.normalizedTable = this.table;

        List<DynamicField> copiedFields = List.copyOf(fields);
        Map<String, DynamicField> indexedFields = new LinkedHashMap<>(FormNames.mapCapacity(copiedFields.size()));
        for (DynamicField field : copiedFields) {
            DynamicField safeField = Objects.requireNonNull(field, "dynamic field must not be null");
            DynamicField previous = indexedFields.putIfAbsent(safeField.normalizedName(), safeField);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate dynamic field name");
            }
        }
        this.fields = copiedFields;
        this.fieldsByName = Map.copyOf(indexedFields);
        this.logicDelete = logicDelete == null ? null : validateLogicDelete(logicDelete);
        this.tenant = tenant == null ? null : validateTenant(tenant);
        this.protections = validateProtections(protections);
        this.structureFingerprint = DynamicFormStructure.fingerprint(
                normalizedTable, this.fields, this.logicDelete, this.tenant, this.protections);
    }

    /**
     * 创建动态表单构建器。
     *
     * @param id    表单 ID
     * @param table 物理表名
     * @return 动态表单构建器
     */
    public static Builder builder(String id, String table) {
        return new Builder(id, table);
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
        return table;
    }

    /**
     * 返回只读字段列表。
     *
     * @return 只读字段列表
     */
    public List<DynamicField> fields() {
        return fields;
    }

    /**
     * 返回只描述 SQL 结构、不包含表单业务 ID、租户值或逻辑删除值的稳定 SHA-256 指纹。
     *
     * <p>指纹在构造时计算一次，字段顺序、物理表、列类型、可空性、主键和数据库生成策略变化都会得到
     * 新指纹；仅更换表单业务 ID 不会破坏等价 SQL 计划复用。该值可安全用于有界结构计划缓存键。</p>
     *
     * @return 64 个十六进制字符的结构指纹
     */
    public String structureFingerprint() {
        return structureFingerprint;
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

    /**
     * 按规范化字段名查找动态字段。
     *
     * @param name 字段名
     * @return 匹配字段
     */
    public Optional<DynamicField> findField(String name) {
        return Optional.ofNullable(fieldsByName.get(FormNames.normalize(name, "dynamic field name")));
    }

    /**
     * 按规范化字段名获取动态字段，不存在时抛出确定性异常。
     *
     * @param name 字段名
     * @return 匹配字段
     */
    public DynamicField field(String name) {
        return findField(name).orElseThrow(() -> new IllegalArgumentException(
                "dynamic field does not exist in form"));
    }

    /**
     * 将动态表单发布为表元数据。
     *
     * @return 表元数据
     */
    public TableMetadata toTableMetadata() {
        TableMetadata.Builder builder = TableMetadata.builder(table);
        for (DynamicField field : fields) {
            builder.addColumn(field.toColumnMetadata());
            if (field.unique()) {
                builder.addIndex(IndexMetadata.builder(DynamicFormStructure.uniqueIndexName(table, field.name()))
                                              .unique()
                                              .addColumn(field.name())
                                              .build());
            }
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
        if (!normalizedTable.equals(safeTarget.normalizedTable)) {
            throw new IllegalArgumentException("cannot diff forms mapped to different tables");
        }

        List<DynamicField> added = new ArrayList<>();
        List<DynamicField> removed = new ArrayList<>();
        List<FieldChange> changed = new ArrayList<>();

        for (DynamicField targetField : safeTarget.fields) {
            DynamicField sourceField = fieldsByName.get(targetField.normalizedName());
            if (sourceField == null) {
                added.add(targetField);
            } else if (!sourceField.equals(targetField)) {
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

    private LogicDeleteDefinition validateLogicDelete(LogicDeleteDefinition definition) {
        if (!fieldsByName.containsKey(FormNames.normalize(definition.fieldName(), "logic delete field name"))) {
            throw new IllegalArgumentException("logic delete field does not exist in form");
        }
        return definition;
    }

    private TenantDefinition validateTenant(TenantDefinition definition) {
        if (!fieldsByName.containsKey(FormNames.normalize(definition.fieldName(), "tenant field name"))) {
            throw new IllegalArgumentException("tenant field does not exist in form");
        }
        return definition;
    }

    private FieldProtectionRegistry validateProtections(FieldProtectionRegistry registry) {
        FieldProtectionRegistry safeRegistry = Objects.requireNonNull(
                registry, "field protection registry must not be null");
        if (safeRegistry.encryptedFields().keySet().stream().anyMatch(name -> !fieldsByName.containsKey(name))
                || safeRegistry.maskedFields().keySet().stream().anyMatch(name -> !fieldsByName.containsKey(name))) {
            throw new IllegalArgumentException("protected field does not exist in form");
        }
        if (safeRegistry.encryptedFields().keySet().stream().anyMatch(this::ormControlField)) {
            // 主键、租户和逻辑删除条件由 ORM 自动生成普通等值谓词，不能到首次执行时才发现无法比较随机密文。
            throw new IllegalArgumentException("encrypted field must not be an ORM control field");
        }
        safeRegistry.encryptedFields().keySet().forEach(this::validateProtectedDataType);
        safeRegistry.maskedFields().keySet().forEach(this::validateProtectedDataType);
        return safeRegistry;
    }

    private boolean ormControlField(String fieldName) {
        DynamicField field = fieldsByName.get(fieldName);
        return field.primaryKey()
                || tenant != null && fieldName.equals(FormNames.normalize(tenant.fieldName(), "tenant field name"))
                || logicDelete != null
                && fieldName.equals(FormNames.normalize(logicDelete.fieldName(), "logic delete field name"));
    }

    private void validateProtectedDataType(String fieldName) {
        String type = fieldsByName.get(fieldName).dataType().trim().toUpperCase(Locale.ROOT);
        if (type.endsWith("[]") || !(type.contains("CHAR") || type.contains("TEXT") || type.contains("CLOB"))) {
            // 加密与 masking 都按稳定文本编码工作，不能把不支持的类型留到首次查询时才失败。
            throw new IllegalArgumentException("protected field must use a textual data type");
        }
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

        private final String table;

        private final List<DynamicField> fields = new ArrayList<>();

        private LogicDeleteDefinition logicDelete;

        private TenantDefinition tenant;

        private final FieldProtectionRegistry.Builder protections = FieldProtectionRegistry.builder();

        private Builder(String id, String table) {
            this.id = FormNames.requireText(id, "dynamic form id");
            this.table = FormNames.requireText(table, "dynamic form table");
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
            return new DynamicForm(id, table, fields, logicDelete, tenant, protections.build());
        }
    }
}
