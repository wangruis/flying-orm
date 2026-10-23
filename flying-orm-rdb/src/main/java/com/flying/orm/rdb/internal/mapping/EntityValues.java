package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.FieldFill;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.condition.TermHandler;
import com.flying.orm.core.condition.TermRegistry;
import com.flying.orm.core.internal.error.ThrowableGraph;
import com.flying.orm.rdb.internal.condition.ConditionNodes;
import com.flying.orm.rdb.mapping.EntityEnumStorage;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.EntityFieldFiller;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.EntityValueWriter;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.protection.ProtectedConditions;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;

/**
 * 把实体拆成“数据库列名 -> Java 值”的有序 Map，供 Repository 写入动态表单。
 *
 * <p>record 读取组件 accessor，普通 bean 优先 getter、缺少 getter 时读取字段。生产热路径由客户端实例的
 * 实体模型注册表缓存反射访问计划。实例构造后只读，可并发共享。</p>
 *
 * @param <T> 实体类型
 * @author wangr
 * @date 2026-07-26
 * @version v1.0
 */
public final class EntityValues<T> {

    private static final DatabaseType VARCHAR_TYPE = DatabaseType.of("VARCHAR");

    private final Map<String, ValueReader> readers;
    private final Map<String, ValueReader> identityReaders;
    private final Map<String, UnaryOperator<Object>> conditionEncoders;

    private final EntityMetadata<T> metadata;
    private final EntityFieldFiller fieldFiller;
    private final Map<String, BiConsumer<Object, Object>> fillWriters;
    private final Object upsertLayoutIdentity;

    private EntityValues(Map<String, ValueReader> readers,
                         Map<String, UnaryOperator<Object>> conditionEncoders,
                         EntityMetadata<T> metadata,
                         EntityFieldFiller fieldFiller) {
        this.readers = readers;
        this.conditionEncoders = conditionEncoders;
        this.metadata = Objects.requireNonNull(metadata, "entity metadata must not be null");
        this.fieldFiller = Objects.requireNonNull(fieldFiller, "entity field filler must not be null");
        this.fillWriters = EntityValueWriter.fillWriters(metadata);
        this.upsertLayoutIdentity = metadata.fields().stream().noneMatch(field ->
                field.primaryKey() && field.generation().generated()
                        || field.insertable() && conditional(field.insertStrategy())
                        || field.updatable() && (conditional(field.updateStrategy())
                                || field.fill() == FieldFill.INSERT_DEFAULT)) ? new Object() : null;
        Map<String, ValueReader> identities = new LinkedHashMap<>();
        readers.forEach((name, reader) -> {
            EntityFieldMetadata field = reader.field();
            if (field.primaryKey() || field.version()) {
                identities.put(name, reader);
            }
        });
        this.identityReaders = identities;
    }

    /** 按读取计划顺序提取全部持久值供保留预算估算；不填充或复制元素，不建立中间字段 Map。 */
    public Object[] readRetainedValues(T entity) {
        T safeEntity = Objects.requireNonNull(entity, "entity must not be null");
        Object[] values = new Object[readers.size()];
        int index = 0;
        for (ValueReader reader : readers.values()) {
            values[index++] = reader.stored(reader.read(safeEntity));
        }
        return values;
    }

    /** 只读取缓存的主键和版本取值计划，不访问无关属性，也不执行写入字段填充。 */
    public Map<String, Object> readIdentity(T entity) {
        T safeEntity = Objects.requireNonNull(entity, "entity must not be null");
        Map<String, Object> values = new LinkedHashMap<>(identityReaders.size());
        identityReaders.forEach((name, reader) -> values.put(name, reader.stored(reader.read(safeEntity))));
        return values;
    }

    /**
     * 读取 insert 能写的字段。数据库生成列、计算列和明确声明 {@code insertable=false} 的字段不会进入 SQL。
     *
     * <p>{@code @TableId(type = AUTO)} 表示值归数据库生成，即使实体字段当前是 null，也不能绑定成 SQL NULL；显式省略该列
     * 才会触发 identity 或 sequence 默认值。生成键读取和回填是另一段执行职责，这里只保证插入参数不会压掉数据库默认值。</p>
     */
    public Map<String, Object> readForInsert(T entity) {
        return read(entity, EntityFieldFiller.Operation.INSERT, field ->
                field.insertable() && !field.generation().generated() ? field.insertStrategy() : FieldStrategy.NEVER);
    }

    /** 读取 update 的 SET 列表，只保留明确允许更新的字段。 */
    public Map<String, Object> readForUpdate(T entity) {
        return read(entity, EntityFieldFiller.Operation.UPDATE,
                field -> field.updatable() ? field.updateStrategy() : FieldStrategy.NEVER);
    }

    /** Repository 的主键和版本由独立定位计划读取，不进入 SET，也不触发其更新填充。 */
    public Map<String, Object> readForRepositoryUpdate(T entity) {
        return read(entity, EntityFieldFiller.Operation.UPDATE, field ->
                field.updatable() && !field.primaryKey() && !field.version()
                        ? field.updateStrategy() : FieldStrategy.NEVER);
    }

    /** 仅读取已选数据库列；沿用实体填充、存储编码及更新策略，不访问其他 getter。 */
    public Map<String, Object> readForUpdate(T entity, Set<String> columns) {
        T safeEntity = Objects.requireNonNull(entity, "entity must not be null");
        Map<String, Object> values = new LinkedHashMap<>();
        for (String column : columns) {
            EntityFieldMetadata field = metadata.field(column);
            if (!field.updatable() || field.updateStrategy() == FieldStrategy.NEVER) {
                continue;
            }
            Object value = readValue(safeEntity, field, readers.get(column), EntityFieldFiller.Operation.UPDATE);
            if (EntityWriteValuePolicy.accepts(field.updateStrategy(), value)) {
                values.put(column, value);
            }
        }
        return values;
    }

    /** Reuse the write plan's enum representation before the shared field/dialect condition encoding. */
    public ConditionGroup normalizeCondition(ConditionGroup where, TermRegistry terms) {
        if (conditionEncoders.isEmpty()) {
            return where;
        }
        return ConditionNodes.rewrite(Objects.requireNonNull(where, "where condition must not be null"),
                term -> normalizeTerm(term, terms));
    }

    /** Reuse the entity field's write representation for one Lambda DML assignment. */
    public Object normalizeWriteValue(String fieldName, Object value) {
        UnaryOperator<Object> encoder = enumEncoder(fieldName);
        return encoder != null && value instanceof Enum<?> ? encoder.apply(value) : value;
    }

    /** Reuse the entity field's condition representation before a multi-source JOIN loses entity identity. */
    public Object normalizeConditionValue(String fieldName,
                                          String operator,
                                          Object value,
                                          TermRegistry terms) {
        ConditionGroup condition = ConditionGroup.and(terms)
                                                 .where(fieldName, operator, value)
                                                 .build();
        TermCondition term = (TermCondition) condition.children().getFirst();
        return encodedConditionValue(term, term.value(), conditionEncoder(term.field(), term.operator(), terms));
    }

    private TermCondition normalizeTerm(TermCondition term, TermRegistry terms) {
        UnaryOperator<Object> encoder = conditionEncoder(term.field(), term.operator(), terms);
        if (encoder == null) {
            return term;
        }
        Object source = term.value();
        Object encoded = encodedConditionValue(term, source, encoder);
        return encoded == source ? term : TermCondition.of(term.identity(), term.operator(), encoded);
    }

    /** 结构化编译器已经整理值形状，此处只映射单个枚举值，不创建或遍历中间条件树。 */
    public Object normalizeConditionScalar(String field, String operator, Object value, TermRegistry terms) {
        if (!(value instanceof Enum<?>)) {
            return value;
        }
        UnaryOperator<Object> encoder = conditionEncoder(field, operator, terms);
        return encoder == null ? value : encoder.apply(value);
    }

    private UnaryOperator<Object> conditionEncoder(String field, String operator, TermRegistry terms) {
        UnaryOperator<Object> encoder = enumEncoder(field);
        if (encoder == null) return null;
        TermHandler standard = TermRegistry.standard().find(operator).orElse(null);
        return ProtectedConditions.EXACT.equals(operator)
                || ProtectedConditions.SUFFIX.equals(operator)
                || ProtectedConditions.CONTAINS.equals(operator)
                || standard != null && terms.find(operator).orElse(null) == standard ? encoder : null;
    }

    private static Object encodedConditionValue(TermCondition term, Object value, UnaryOperator<Object> encoder) {
        if (encoder == null) return value;
        if (ProtectedConditions.EXACT.equals(term.operator())
                || ProtectedConditions.SUFFIX.equals(term.operator())
                || ProtectedConditions.CONTAINS.equals(term.operator())) {
            return value instanceof Enum<?> ? encoder.apply(value) : value;
        }
        TermHandler standard = TermRegistry.standard().find(term.operator()).orElse(null);
        return switch (standard.shape()) {
            case SCALAR -> value instanceof Enum<?> ? encoder.apply(value) : value;
            case COLLECTION, RANGE -> encodeEnumElements(value, encoder);
            default -> value;
        };
    }

    private UnaryOperator<Object> enumEncoder(String fieldName) {
        EntityFieldMetadata field = metadata.findField(fieldName).orElse(null);
        return field == null ? null : conditionEncoders.get(field.columnName());
    }

    private static Object encodeEnumElements(Object source, UnaryOperator<Object> encoder) {
        if (!(source instanceof List<?> values)) {
            return source;
        }
        List<Object> encoded = null;
        for (int index = 0; index < values.size(); index++) {
            Object value = values.get(index);
            Object next = value instanceof Enum<?> ? encoder.apply(value) : value;
            if (encoded == null && next != value) {
                encoded = new ArrayList<>(values.size());
                encoded.addAll(values.subList(0, index));
            }
            if (encoded != null) {
                encoded.add(next);
            }
        }
        return encoded == null ? source : encoded;
    }

    /**
     * 为 Repository 批量 upsert 一次读取实体，同时保留 INSERT 与冲突 UPDATE 的独立字段策略。
     *
     * @param entity 本次写入的实体
     * @return 不可变的阶段化 upsert 快照
     */
    public RepositoryUpsertValues repositoryUpsertValues(T entity) {
        T safeEntity = Objects.requireNonNull(entity, "entity must not be null");
        Map<String, Object> values = new LinkedHashMap<>();
        Map<String, Object> insertValues = values;
        Map<String, Object> updateValues = values;
        for (Map.Entry<String, ValueReader> entry : readers.entrySet()) {
            String name = entry.getKey();
            ValueReader reader = entry.getValue();
            EntityFieldMetadata field = reader.field();
            boolean generatedIdentity = field.primaryKey() && field.generation().generated();
            boolean insert = field.insertable() && !field.generation().generated()
                    && field.insertStrategy() != FieldStrategy.NEVER;
            boolean update = field.updatable() && field.updateStrategy() != FieldStrategy.NEVER;
            if (!insert && !update && !generatedIdentity) {
                continue;
            }
            Object original = reader.read(safeEntity);
            if (generatedIdentity) {
                insert = original != null;
                update = false;
                if (!insert) continue;
            }
            boolean insertDefault = field.fill() == FieldFill.INSERT_DEFAULT;
            Object filled = generatedIdentity || insertDefault && !insert ? original : EntityWriteValuePolicy.fill(
                    safeEntity, field, EntityFieldFiller.Operation.UPSERT, original, fieldFiller);
            Object value = reader.stored(safeEntity, fillWriters, original, filled);
            insert &= generatedIdentity || EntityWriteValuePolicy.accepts(field.insertStrategy(), value);
            update &= !(insertDefault && original == null && filled != null)
                    && EntityWriteValuePolicy.accepts(field.updateStrategy(), value);
            if (!insert && !update) {
                continue;
            }
            // 阶段字段与并集相同就共用；遇到首个不属于该阶段的字段时，才复制已有前缀。
            if (!insert && insertValues == values) {
                insertValues = new LinkedHashMap<>(values);
            }
            if (!update && updateValues == values) {
                updateValues = new LinkedHashMap<>(values);
            }
            values.put(name, value);
            if (insert && insertValues != values) {
                insertValues.put(name, value);
            }
            if (update && updateValues != values) {
                updateValues.put(name, value);
            }
        }
        return new RepositoryUpsertValues(values, insertValues, updateValues, upsertLayoutIdentity);
    }

    private static boolean conditional(FieldStrategy strategy) {
        return strategy == FieldStrategy.NOT_NULL || strategy == FieldStrategy.NOT_EMPTY;
    }

    private Map<String, Object> read(T entity,
                                     EntityFieldFiller.Operation operation,
                                     Function<EntityFieldMetadata, FieldStrategy> strategy) {
        T safeEntity = Objects.requireNonNull(entity, "entity must not be null");
        Map<String, Object> values = new LinkedHashMap<>();
        // LinkedHashMap 保留元数据声明顺序，让单行和批量 SQL 都能得到稳定列顺序。
        readers.forEach((name, reader) -> {
            EntityFieldMetadata field = reader.field();
            FieldStrategy writeStrategy = strategy.apply(field);
            if (writeStrategy == FieldStrategy.NEVER) {
                return;
            }
            Object value = readValue(safeEntity, field, reader, operation);
            if (EntityWriteValuePolicy.accepts(writeStrategy, value)) {
                values.put(name, value);
            }
        });
        return values;
    }

    private Object readValue(T entity, EntityFieldMetadata field, ValueReader reader,
                              EntityFieldFiller.Operation operation) {
        Object original = reader.read(entity);
        Object filled = EntityWriteValuePolicy.fill(entity, field, operation, original, fieldFiller);
        return reader.stored(entity, fillWriters, original, filled);
    }

    /** 仅供内部测试或注册表缓存未命中时创建计划。 */
    public static <T> EntityValues<T> createUncached(Class<T> type) {
        return createUncached(type, EntityMetadataResolver.createUncached(type));
    }

    /** 注册表已解析元数据时复用它，避免同一次缓存 miss 重复扫描实体。 */
    public static <T> EntityValues<T> createUncached(Class<T> type, EntityMetadata<T> metadata) {
        return createUncached(type, metadata, EntityFieldFiller.none());
    }

    /** 创建使用显式字段填充器的未缓存计划，生产调用通常由 EntityModelRegistry 完成。 */
    public static <T> EntityValues<T> createUncached(Class<T> type,
                                                     EntityMetadata<T> metadata,
                                                     EntityFieldFiller fieldFiller) {
        return type.isRecord() ? recordValues(type, metadata, fieldFiller)
                : beanValues(type, metadata, fieldFiller);
    }

    private static <T> EntityValues<T> recordValues(Class<T> type,
                                                    EntityMetadata<T> metadata,
                                                    EntityFieldFiller fieldFiller) {
        Map<String, ValueReader> readers = new LinkedHashMap<>();
        Map<String, UnaryOperator<Object>> conditionEncoders = new LinkedHashMap<>();
        for (RecordComponent component : type.getRecordComponents()) {
            // findField 同时支持列别名；反射成员只能按真实 Java 属性名选择持久化元数据。
            EntityFieldMetadata field = metadata.findField(component.getName()).orElse(null);
            if (field == null || !field.name().equals(component.getName())) {
                // exist=false 的组件仍是 record 构造参数，但它不属于数据库模型，读取计划必须直接跳过。
                continue;
            }
            Method accessor = component.getAccessor();
            requireAccessible(accessor, "record accessor");
            // 这里的 key 必须是数据库列名。Repository 后面会拿它去找 DynamicForm 字段并渲染 SQL。
            readers.put(field.columnName(), stored(new MethodValueReader(accessor, field), field, component.getType(),
                                                   conditionEncoders));
        }
        return new EntityValues<>(readers, conditionEncoders, metadata, fieldFiller);
    }

    private static <T> EntityValues<T> beanValues(Class<T> type,
                                                  EntityMetadata<T> metadata,
                                                  EntityFieldFiller fieldFiller) {
        try {
            Map<String, ValueReader> readers = new LinkedHashMap<>();
            Map<String, UnaryOperator<Object>> conditionEncoders = new LinkedHashMap<>();
            Set<String> readProperties = new HashSet<>();
            // getter 优先，允许实体自己提供计算或归一化后的持久值。
            for (PropertyDescriptor property : Introspector.getBeanInfo(type).getPropertyDescriptors()) {
                Method readMethod = property.getReadMethod();
                EntityFieldMetadata field = metadata.findField(property.getName()).orElse(null);
                if (readMethod != null && field != null
                        && EntityFieldNames.matches(field.name(), property.getName())
                        && EntityMetadataHierarchy.isPersistentAccessor(readMethod, property.getName())
                        && !"class".equals(property.getName())) {
                    requireAccessible(readMethod, "bean getter");
                    // Java 里叫 name，表里可能叫 user_name。写库时直接交出列名，调用方不用自己转。
                    readers.put(field.columnName(), stored(new MethodValueReader(readMethod, field), field,
                                                           EntityMetadataHierarchy.memberClass(type, readMethod.getGenericReturnType()),
                                                           conditionEncoders));
                    readProperties.add(field.name());
                }
            }
            for (Field field : EntityMetadataHierarchy.persistentFields(type)) {
                EntityFieldMetadata persistentField = metadata.findField(field.getName()).orElse(null);
                if (persistentField != null
                        && persistentField.name().equals(field.getName())
                        && !readProperties.contains(field.getName())) {
                    requireAccessible(field, "bean field");
                    // 没有 getter 的字段也按同一套实体元数据走，避免 Bean 和 record 表现不一致。
                    readers.put(persistentField.columnName(),
                                stored(new FieldValueReader(field, persistentField), persistentField,
                                       EntityMetadataHierarchy.memberClass(type, field.getGenericType()),
                                       conditionEncoders));
                }
            }
            return new EntityValues<>(readers, conditionEncoders, metadata, fieldFiller);
        } catch (IntrospectionException error) {
            throw new MappingException("entity values cannot be created for " + type.getName(), error);
        }
    }

    public int logicalWeight() {
        // 与元数据、行映射计划统一使用“反射槽”计量，避免固定头部被三种派生模型重复收费。
        return Math.max(1, readers.size() + fillWriters.size());
    }

    /**
     * 映射计划创建时就确认反射访问权限，避免第一条数据写入时才暴露 JPMS 包未开放问题。
     * 错误统一为 MappingException，调用方不需要理解 JDK 内部的反射异常类型。
     */
    private static void requireAccessible(AccessibleObject member, String role) {
        if (!member.trySetAccessible()) {
            throw new MappingException(role + " is not accessible; open the entity package or expose a public member: "
                                               + member);
        }
    }

    private interface ValueReader {

        EntityFieldMetadata field();

        Object read(Object target);

        default Object stored(Object value) {
            return value;
        }

        default Object stored(Object entity, Map<String, BiConsumer<Object, Object>> writers,
                              Object original, Object filled) {
            Object stored = stored(filled);
            if (filled != original && !writers.isEmpty()) {
                BiConsumer<Object, Object> writer = writers.get(field().name());
                if (writer != null) writer.accept(entity, filled);
            }
            return stored;
        }
    }

    private record EncodedValueReader(ValueReader source,
                                      UnaryOperator<Object> encoder) implements ValueReader {

        @Override
        public EntityFieldMetadata field() {
            return source.field();
        }

        @Override
        public Object read(Object target) {
            return source.read(target);
        }

        @Override
        public Object stored(Object value) {
            return encoder.apply(value);
        }
    }

    private static ValueReader stored(ValueReader reader,
                                       EntityFieldMetadata field,
                                       Class<?> javaType,
                                       Map<String, UnaryOperator<Object>> conditionEncoders) {
        boolean customMapping = field.customMapping() != null;
        UnaryOperator<Object> enumEncoder;
        String enumMember = field.enumValueMember();
        if (enumMember != null) {
            EntityEnumValueCodec codec = EntityEnumValueCodec.create(javaType, enumMember, field.databaseType());
            enumEncoder = value -> javaType.isInstance(value) ? codec.write(value) : value;
        } else if (customMapping) {
            return reader;
        } else {
            EntityEnumStorage storage = field.enumStorage();
            enumEncoder = storage == EntityEnumStorage.NONE
                    ? null
                    : value -> {
                        if (!(value instanceof Enum<?> enumValue)) {
                            return value;
                        }
                        return storage == EntityEnumStorage.ORDINAL ? enumValue.ordinal() : enumValue.name();
                    };
        }
        if (enumEncoder != null) {
            UnaryOperator<Object> storedEncoder = customMapping || !VARCHAR_TYPE.equals(field.databaseType())
                    ? enumEncoder : value -> textBackedValue(enumEncoder.apply(value));
            conditionEncoders.put(field.columnName(), storedEncoder);
            return new EncodedValueReader(reader, storedEncoder);
        }
        // @EnumValue 的映射针对声明成员；提取后同样必须保留其原始类型交给自定义 codec。
        if (customMapping || !VARCHAR_TYPE.equals(field.databaseType())) {
            return reader;
        }
        // 这些标准 Java 类型由实体模型明确落为跨方言 VARCHAR；Repository 自动生成的主键/乐观锁条件
        // 会直接复用本 Map，因此必须在这里统一成与普通写入相同的数据库文本形态。
        return new EncodedValueReader(reader, EntityValues::textBackedValue);
    }

    private static Object textBackedValue(Object value) {
        if (value instanceof char[] characters) {
            return new String(characters);
        }
        if (value instanceof CharSequence
                || value instanceof Character
                || value instanceof UUID
                || value instanceof Instant
                || value instanceof OffsetDateTime) {
            return value.toString();
        }
        // 未知业务类型仍交给应用自定义 codec，不能因为字段默认推断为 VARCHAR 就提前丢失运行时类型。
        return value;
    }

    private record MethodValueReader(Method method, EntityFieldMetadata field) implements ValueReader {

        @Override
        public Object read(Object target) {
            try {
                return method.invoke(target);
            } catch (ReflectiveOperationException error) {
                ThrowableGraph.rethrowVirtualMachineError(error);
                throw new MappingException("entity getter cannot be read: " + method.getName(), error);
            }
        }
    }

    private record FieldValueReader(Field member, EntityFieldMetadata field) implements ValueReader {

        @Override
        public Object read(Object target) {
            try {
                return member.get(target);
            } catch (IllegalAccessException error) {
                throw new MappingException("entity field cannot be read: " + member.getName(), error);
            }
        }
    }
}
