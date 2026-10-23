package com.flying.orm.rdb.mapping;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.internal.hash.StableDigest;
import com.flying.orm.core.internal.hash.StableEncoder;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.json.JsonValueCodec;
import tools.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * 实体 Java 类型、规范数据库类型和字段 codec 的只读绑定表。
 *
 * <p>注册表只在实体关系元数据编译时解析类型；编译后的映射和 codec 注册表可以直接并发复用，
 * 不给 CRUD 热路径增加类型查找或临时对象。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class EntityTypeMappingRegistry {

    private static final StableDigest.Domain FINGERPRINT_DOMAIN =
            StableDigest.domain("entity-type-mapping-registry/v1");

    private static final ValueCodec STANDARD_CODEC = new StandardCodecAdapter();
    private static final ValueCodec JSON_CODEC = new JsonCodecAdapter();
    private static final List<Mapping> STANDARD_MAPPINGS = standardMappings();
    private static final EntityTypeMappingRegistry STANDARD =
            new EntityTypeMappingRegistry(STANDARD_MAPPINGS, List.of(), List.of());

    private final List<Mapping> mappings;
    private final Map<String, List<Mapping>> mappingsById;
    private final List<Mapping> customMappings;
    private final String fingerprint;
    private final ValueCodecRegistry valueCodecs;

    private EntityTypeMappingRegistry(Collection<Mapping> mappings,
                                      List<Mapping> customMappings,
                                      List<Mapping> namedMappings) {
        List<Mapping> safeMappings = List.copyOf(mappings);
        this.mappings = safeMappings;
        this.customMappings = List.copyOf(customMappings);
        List<Mapping> allMappings = new ArrayList<>(safeMappings);
        allMappings.addAll(namedMappings);
        this.mappingsById = indexById(allMappings);
        this.fingerprint = namedMappings.isEmpty() ? fingerprint(safeMappings)
                : StableDigest.sha256(FINGERPRINT_DOMAIN)
                        .text("DEFAULT_MAPPINGS", fingerprint(safeMappings))
                        .text("NAMED_MAPPINGS", fingerprint(namedMappings)).finishHex();
        this.valueCodecs = valueCodecs(ValueCodecRegistry.standard());
    }

    /** 返回内置映射的共享实例。 */
    public static EntityTypeMappingRegistry standard() {
        return STANDARD;
    }

    /** 创建已经包含全部内置映射的新 builder。 */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * 严格解析 Java 类型。未知业务类型不会猜测为 VARCHAR，必须先显式注册。
     *
     * @param javaType 实体属性的 Java 类型
     * @return 完整类型映射
     */
    public Mapping resolve(Class<?> javaType) {
        return find(javaType).orElseThrow(() ->
                new MappingException("no entity type mapping for Java type " + javaType.getTypeName()));
    }

    /** 查询类型绑定；未注册的业务类型可由 CRUD 使用外部 codec，Schema 则要求显式绑定。 */
    public Optional<Mapping> find(Class<?> javaType) {
        Class<?> safeType = boxed(Objects.requireNonNull(javaType, "entity Java type must not be null"));
        Mapping registered = EntityTypeMappingResolver.resolveRegistered(
                mappings, safeType, true, null, STANDARD_MAPPINGS);
        if (registered != null) {
            return Optional.of(registered);
        }
        if (safeType.isEnum()) {
            return Optional.of(new Mapping("VARCHAR", safeType, DatabaseType.of("VARCHAR"), STANDARD_CODEC));
        }
        return Optional.empty();
    }

    /**
     * 按声明的稳定 ID 解析并同时校验 Java 类型，避免注解和 codec 指向两套类型语义。
     *
     * @param id       TableColumn 声明的映射 ID
     * @param javaType 实体属性的 Java 类型
     * @return ID 和 Java 类型一致的完整映射
     */
    public Mapping resolve(String id, Class<?> javaType) {
        String safeId = requireId(id);
        Class<?> safeType = boxed(Objects.requireNonNull(javaType, "entity Java type must not be null"));
        Mapping registered = EntityTypeMappingResolver.resolveRegistered(
                mappingsById.getOrDefault(safeId, List.of()),
                safeType,
                false,
                safeId,
                STANDARD_MAPPINGS);
        if (registered != null) {
            return registered;
        }
        if (safeType.isEnum() && "VARCHAR".equals(safeId)) {
            return new Mapping("VARCHAR", safeType, DatabaseType.of("VARCHAR"), STANDARD_CODEC);
        }
        throw new MappingException("entity type mapping id " + safeId + " does not support Java type "
                                           + safeType.getTypeName());
    }

    /** 相同注册内容产生相同摘要；全部映射及注册顺序都被纳入，顺序继续决定自定义 codec 优先级。 */
    public String fingerprint() {
        return fingerprint;
    }

    /** 返回构造时一次生成的只读 codec 注册表，自定义 codec 始终先于内置转换。 */
    public ValueCodecRegistry valueCodecs() {
        return valueCodecs;
    }

    /**
     * 把本注册表的实体 codec 放到现有应用 codec 前面，同时保留调用方已经配置的驱动适配器和其他转换。
     *
     * <p>这个组合只在客户端启动期执行一次，只包含默认类型映射。具名字段映射由实体描述精确绑定到字段，
     * 不注入全局类型转换，也不在逐字段绑定时查表或猜测 {@code databaseTypeId}。</p>
     */
    public ValueCodecRegistry valueCodecs(ValueCodecRegistry fallback) {
        return Objects.requireNonNull(fallback, "fallback value codec registry must not be null")
                      .withFirst(extensionCodecs(customMappings).toArray(ValueCodec[]::new));
    }

    /** 内置映射及 codec 身份只在注册表初始化时创建一次。 */
    private static List<Mapping> standardMappings() {
        List<Mapping> mappings = new ArrayList<>(22);
        addStandardMapping(mappings, "JSON", Map.class, JSON_CODEC);
        addStandardMapping(mappings, "JSON", Collection.class, JSON_CODEC);
        addStandardMapping(mappings, "JSON", JsonNode.class, JSON_CODEC);
        addStandardMapping(mappings, "VARCHAR", String.class, STANDARD_CODEC);
        addStandardMapping(mappings, "BIGINT", Long.class, STANDARD_CODEC);
        addStandardMapping(mappings, "INTEGER", Integer.class, STANDARD_CODEC);
        addStandardMapping(mappings, "INTEGER", Short.class, STANDARD_CODEC);
        addStandardMapping(mappings, "INTEGER", Byte.class, STANDARD_CODEC);
        addStandardMapping(mappings, "BOOLEAN", Boolean.class, STANDARD_CODEC);
        addStandardMapping(mappings, "DECIMAL", BigDecimal.class, STANDARD_CODEC);
        addStandardMapping(mappings, "DECIMAL", BigInteger.class, STANDARD_CODEC);
        addStandardMapping(mappings, "DECIMAL", Double.class, STANDARD_CODEC);
        addStandardMapping(mappings, "DECIMAL", Float.class, STANDARD_CODEC);
        addStandardMapping(mappings, "TIMESTAMP", LocalDateTime.class, STANDARD_CODEC);
        addStandardMapping(mappings, "TIMESTAMPTZ", Instant.class, STANDARD_CODEC);
        addStandardMapping(mappings, "TIMESTAMPTZ", OffsetDateTime.class, STANDARD_CODEC);
        addStandardMapping(mappings, "DATE", LocalDate.class, STANDARD_CODEC);
        addStandardMapping(mappings, "TIME", LocalTime.class, STANDARD_CODEC);
        addStandardMapping(mappings, "OFFSET_TIME", OffsetTime.class, STANDARD_CODEC);
        addStandardMapping(mappings, "UUID", UUID.class, STANDARD_CODEC);
        addStandardMapping(mappings, "BINARY", byte[].class, STANDARD_CODEC);
        addStandardMapping(mappings, "BINARY", Byte[].class, STANDARD_CODEC);
        return List.copyOf(mappings);
    }

    private static void addStandardMapping(List<Mapping> mappings,
                                           String id,
                                           Class<?> javaType,
                                           ValueCodec codec) {
        mappings.add(new Mapping(id, javaType, DatabaseType.of(id), codec));
    }

    private static Map<String, List<Mapping>> indexById(List<Mapping> mappings) {
        Set<MappingKey> keys = new HashSet<>();
        Map<String, List<Mapping>> byId = new LinkedHashMap<>();
        for (Mapping mapping : mappings) {
            if (!keys.add(new MappingKey(mapping.id(), mapping.javaType()))) {
                throw new IllegalArgumentException("entity type mapping id " + mapping.id()
                        + " already exists for Java type " + mapping.javaType().getTypeName());
            }
            byId.computeIfAbsent(mapping.id(), ignored -> new ArrayList<>()).add(mapping);
        }
        Map<String, List<Mapping>> immutable = new LinkedHashMap<>();
        for (Map.Entry<String, List<Mapping>> entry : byId.entrySet()) {
            immutable.put(entry.getKey(), List.copyOf(entry.getValue()));
        }
        return Collections.unmodifiableMap(immutable);
    }

    private record MappingKey(String id, Class<?> javaType) {
    }

    private static IllegalArgumentException duplicateMapping(Mapping mapping) {
        return new IllegalArgumentException("entity type mapping already exists for Java type "
                                                    + mapping.javaType().getTypeName());
    }

    private static String fingerprint(List<Mapping> mappings) {
        StableEncoder encoder = StableDigest.sha256(FINGERPRINT_DOMAIN)
                                            .integer("MAPPING_COUNT", mappings.size());
        for (Mapping mapping : mappings) {
            encoder.marker("MAPPING")
                   .text("ID", mapping.id())
                   .text("JAVA_TYPE", mapping.javaType().getName())
                   .text("DATABASE_TYPE", mapping.databaseType().canonical())
                   .text("CODEC_TYPE", mapping.codec().getClass().getName());
        }
        return encoder.finishHex();
    }

    private static List<ValueCodec> extensionCodecs(List<Mapping> customMappings) {
        List<ValueCodec> codecs = new ArrayList<>(customMappings.size() + 1);
        Set<ValueCodec> added = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Mapping mapping : customMappings) {
            if (added.add(mapping.codec())) {
                codecs.add(mapping.codec());
            }
        }
        codecs.add(JSON_CODEC);
        return List.copyOf(codecs);
    }

    private static String requireId(String id) {
        String safeId = Objects.requireNonNull(id, "entity type mapping id must not be null").trim();
        if (safeId.isEmpty()) {
            throw new IllegalArgumentException("entity type mapping id must not be blank");
        }
        return safeId;
    }

    private static Class<?> boxed(Class<?> type) {
        if (!type.isPrimitive()) {
            return type;
        }
        if (type == long.class) {
            return Long.class;
        }
        if (type == int.class) {
            return Integer.class;
        }
        if (type == short.class) {
            return Short.class;
        }
        if (type == byte.class) {
            return Byte.class;
        }
        if (type == boolean.class) {
            return Boolean.class;
        }
        if (type == double.class) {
            return Double.class;
        }
        if (type == float.class) {
            return Float.class;
        }
        return type;
    }

    /** 一个已经完成校验、可直接供实体关系编译器消费的只读映射。 */
    public static final class Mapping {

        private final String id;
        private final Class<?> javaType;
        private final DatabaseType databaseType;
        private final ValueCodec codec;
        private final boolean custom;

        Mapping(String id, Class<?> javaType, DatabaseType databaseType, ValueCodec codec) {
            this(id, javaType, databaseType, codec, false);
        }

        private Mapping(String id,
                        Class<?> javaType,
                        DatabaseType databaseType,
                        ValueCodec codec,
                        boolean custom) {
            this.id = requireId(id);
            this.javaType = boxed(Objects.requireNonNull(javaType, "mapped Java type must not be null"));
            this.databaseType = Objects.requireNonNull(databaseType, "mapped database type must not be null")
                                       .requireSafe("entity type mapping " + this.id);
            this.codec = Objects.requireNonNull(codec, "mapped value codec must not be null");
            this.custom = custom;
            if (!this.codec.supports(this.javaType)) {
                throw new IllegalArgumentException("value codec " + this.codec.getClass().getName()
                                                           + " does not support " + this.javaType.getTypeName());
            }
        }

        public String id() {
            return id;
        }

        public Class<?> javaType() {
            return javaType;
        }

        public DatabaseType databaseType() {
            return databaseType;
        }

        public ValueCodec codec() {
            return codec;
        }

        boolean custom() {
            return custom;
        }

        /** 把注解文本先还原成 Java 值，再编码成数据库可绑定的标量。 */
        public Object readLiteral(String literal) {
            Object value = codec.read(Objects.requireNonNull(literal, "mapping literal must not be null"),
                                      javaType);
            return codec.write(value);
        }

        Mapping forResolvedType(Class<?> resolvedType) {
            return javaType == resolvedType
                    ? this : new Mapping(id, resolvedType, databaseType, codec, custom);
        }
    }

    /** 只在启动或首次实体编译前使用；build 后的注册表不再变化。 */
    public static final class Builder {

        private final Map<Class<?>, Mapping> mappings = new LinkedHashMap<>();
        private final List<Mapping> customMappings = new ArrayList<>();
        private final List<Mapping> namedMappings = new ArrayList<>();

        private Builder() {
            for (Mapping mapping : STANDARD_MAPPINGS) {
                mappings.put(mapping.javaType(), mapping);
            }
        }

        /** 注册一份不可拆分的 Java 类型、数据库类型和 codec 绑定。 */
        public Builder register(String id,
                                Class<?> javaType,
                                DatabaseType databaseType,
                                ValueCodec codec) {
            Mapping mapping = new Mapping(id, javaType, databaseType, codec, true);
            if (mappings.putIfAbsent(mapping.javaType(), mapping) != null) {
                throw duplicateMapping(mapping);
            }
            customMappings.add(mapping);
            return this;
        }

        public EntityTypeMappingRegistry build() {
            return new EntityTypeMappingRegistry(mappings.values(), customMappings, namedMappings);
        }

        /**
         * 注册由 {@code @TableColumn(databaseTypeId=...)} 显式选择的字段映射。
         * 同一 Java 类型可以有多个不同 ID；不改变该类型的默认映射或应用级 codec。
         * 字段使用 {@code @EnumValue} 时，映射的 Java 类型是枚举值成员的类型。
         */
        public Builder registerNamed(String id,
                                     Class<?> javaType,
                                     DatabaseType databaseType,
                                     ValueCodec codec) {
            namedMappings.add(new Mapping(id, javaType, databaseType, codec, true));
            return this;
        }
    }

    private static final class StandardCodecAdapter implements ValueCodec {

        private final ValueCodecRegistry delegate = ValueCodecRegistry.standard();

        @Override
        public boolean supports(Class<?> targetType) {
            Objects.requireNonNull(targetType, "codec target type must not be null");
            // 作为末位适配器接住剩余类型，是否支持仍由 core 标准注册表统一裁决。
            return true;
        }

        @Override
        public Object write(Object value) {
            return delegate.write(value);
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return delegate.read(value, targetType);
        }
    }

    private static final class JsonCodecAdapter implements ValueCodec {

        @Override
        public java.util.Optional<com.flying.orm.core.codec.ValueCodecDescriptor> descriptor() {
            return java.util.Optional.of(JsonValueCodec.descriptor());
        }

        @Override
        public boolean supports(Class<?> targetType) {
            return JsonValueCodec.supportsTarget(targetType);
        }

        @Override
        public Object write(Object value) {
            return JsonValueCodec.write(value);
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return JsonValueCodec.read(value, targetType);
        }
    }

}
