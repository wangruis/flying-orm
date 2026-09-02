package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.core.internal.error.ThrowableGraph;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.mapping.EntityEnumStorage;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.EntityFieldFiller;
import com.flying.orm.rdb.mapping.EntityMappingEvent;
import com.flying.orm.rdb.mapping.EntityMappingListener;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.MappingException;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.RecordComponent;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiPredicate;

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

    private final EntityMetadata<T> metadata;
    private final EntityFieldFiller fieldFiller;

    private EntityValues(Map<String, ValueReader> readers,
                         EntityMetadata<T> metadata,
                         EntityFieldFiller fieldFiller) {
        this.readers = Collections.unmodifiableMap(new LinkedHashMap<>(readers));
        this.metadata = Objects.requireNonNull(metadata, "entity metadata must not be null");
        this.fieldFiller = Objects.requireNonNull(fieldFiller, "entity field filler must not be null");
    }

    /**
     * 读取一份实体快照，返回按实体元数据顺序排列的“数据库列名 -> Java 值”。
     *
     * @param entity 本次写入的实体
     * @return 新建的可修改 Map；修改它不会反向修改实体或缓存计划
     * @throws MappingException getter 或字段无法读取时抛出
     */
    public Map<String, Object> read(T entity) {
        return read(entity, null, (field, value) -> true);
    }

    /**
     * 读取 insert 能写的字段。数据库生成列、计算列和明确声明 {@code insertable=false} 的字段不会进入 SQL。
     *
     * <p>{@code @TableId(type = AUTO)} 表示值归数据库生成，即使实体字段当前是 null，也不能绑定成 SQL NULL；显式省略该列
     * 才会触发 identity 或 sequence 默认值。生成键读取和回填是另一段执行职责，这里只保证插入参数不会压掉数据库默认值。</p>
     */
    public Map<String, Object> readForInsert(T entity) {
        return read(entity, EntityFieldFiller.Operation.INSERT, (field, value) -> field.insertable()
                && !field.generation().generated()
                && EntityWriteValuePolicy.accepts(field.insertStrategy(), value));
    }

    /** 读取 update 的 SET 列表，只保留明确允许更新的字段。 */
    public Map<String, Object> readForUpdate(T entity) {
        return read(entity, EntityFieldFiller.Operation.UPDATE,
                    (field, value) -> field.updatable()
                            && EntityWriteValuePolicy.accepts(field.updateStrategy(), value));
    }

    /**
     * upsert 同时包含插入和冲突更新两个阶段，只要字段允许其中一种写法就保留；具体阶段仍由方言渲染器决定。
     */
    public Map<String, Object> readForUpsert(T entity) {
        return read(entity, EntityFieldFiller.Operation.UPSERT,
                    (field, value) -> (field.insertable()
                            && EntityWriteValuePolicy.accepts(field.insertStrategy(), value))
                            || (field.updatable()
                            && EntityWriteValuePolicy.accepts(field.updateStrategy(), value)));
    }

    /**
     * 为 Repository 批量 upsert 一次读取实体，同时保留 INSERT 与冲突 UPDATE 的独立字段策略。
     *
     * @param entity 本次写入的实体
     * @return 不可变的阶段化 upsert 快照
     */
    @InternalApi
    public RepositoryUpsertValues repositoryUpsertValues(T entity) {
        T safeEntity = Objects.requireNonNull(entity, "entity must not be null");
        Map<String, Object> values = new LinkedHashMap<>();
        Map<String, Object> insertValues = new LinkedHashMap<>();
        Map<String, Object> updateValues = new LinkedHashMap<>();
        readers.forEach((name, reader) -> {
            EntityFieldMetadata field = metadata.field(name);
            Object value = EntityWriteValuePolicy.fill(
                    safeEntity,
                    field,
                    EntityFieldFiller.Operation.UPSERT,
                    reader.read(safeEntity),
                    fieldFiller);
            boolean insert = field.insertable()
                    && EntityWriteValuePolicy.accepts(field.insertStrategy(), value);
            boolean update = field.updatable()
                    && EntityWriteValuePolicy.accepts(field.updateStrategy(), value);
            if (insert) {
                insertValues.put(name, value);
            }
            if (update) {
                updateValues.put(name, value);
            }
            if (insert || update) {
                values.put(name, value);
            }
        });
        return new RepositoryUpsertValues(values, insertValues, updateValues);
    }

    private Map<String, Object> read(T entity,
                                     EntityFieldFiller.Operation operation,
                                     BiPredicate<EntityFieldMetadata, Object> included) {
        T safeEntity = Objects.requireNonNull(entity, "entity must not be null");
        Map<String, Object> values = new LinkedHashMap<>();
        // LinkedHashMap 保留元数据声明顺序，让单行和批量 SQL 都能得到稳定列顺序。
        readers.forEach((name, reader) -> {
            EntityFieldMetadata field = metadata.field(name);
            Object value = reader.read(safeEntity);
            value = EntityWriteValuePolicy.fill(safeEntity, field, operation, value, fieldFiller);
            if (included.test(field, value)) {
                values.put(name, value);
            }
        });
        return values;
    }

    /**
     * 读取实体并在参数交给 SQL 渲染器前发布一次事件。默认 {@link #read(Object)} 不创建事件对象，
     * 只有明确安装监听器的应用才承担这点开销。
     *
     * @param entity 本次写入的实体
     * @param listener 应用级线程安全监听器
     * @return 新建的可修改列值 Map
     */
    public Map<String, Object> read(T entity, EntityMappingListener listener) {
        EntityMappingListener safeListener = Objects.requireNonNull(listener,
                                                                    "entity mapping listener must not be null");
        Map<String, Object> values = read(entity);
        if (safeListener != EntityMappingListener.NONE) {
            safeListener.beforeWrite(new EntityMappingEvent(metadata, entity, values));
        }
        return values;
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
        if (type.isRecord()) {
            return recordValues(type, metadata, fieldFiller);
        }
        return beanValues(type, metadata, fieldFiller);
    }

    private static <T> EntityValues<T> recordValues(Class<T> type,
                                                    EntityMetadata<T> metadata,
                                                    EntityFieldFiller fieldFiller) {
        Map<String, ValueReader> readers = new LinkedHashMap<>();
        for (RecordComponent component : type.getRecordComponents()) {
            EntityFieldMetadata field = metadata.findField(component.getName()).orElse(null);
            if (field == null) {
                // exist=false 的组件仍是 record 构造参数，但它不属于数据库模型，读取计划必须直接跳过。
                continue;
            }
            Method accessor = component.getAccessor();
            requireAccessible(accessor, "record accessor");
            // 这里的 key 必须是数据库列名。Repository 后面会拿它去找 DynamicForm 字段并渲染 SQL。
            readers.put(field.columnName(), stored(new MethodValueReader(accessor), field, component.getType()));
        }
        return new EntityValues<>(readers, metadata, fieldFiller);
    }

    private static <T> EntityValues<T> beanValues(Class<T> type,
                                                  EntityMetadata<T> metadata,
                                                  EntityFieldFiller fieldFiller) {
        try {
            Map<String, ValueReader> readers = new LinkedHashMap<>();
            Set<String> readProperties = new HashSet<>();
            // getter 优先，允许实体自己提供计算或归一化后的持久值。
            for (PropertyDescriptor property : Introspector.getBeanInfo(type).getPropertyDescriptors()) {
                Method readMethod = property.getReadMethod();
                EntityFieldMetadata field = metadata.findField(property.getName()).orElse(null);
                if (readMethod != null && field != null && !"class".equals(property.getName())) {
                    requireAccessible(readMethod, "bean getter");
                    // Java 里叫 name，表里可能叫 user_name。写库时直接交出列名，调用方不用自己转。
                    readers.put(field.columnName(), stored(new MethodValueReader(readMethod), field,
                                                           readMethod.getReturnType()));
                    readProperties.add(property.getName());
                }
            }
            for (Field field : persistentFields(type)) {
                EntityFieldMetadata persistentField = metadata.findField(field.getName()).orElse(null);
                if (!Modifier.isStatic(field.getModifiers())
                        && persistentField != null
                        && !readProperties.contains(field.getName())) {
                    requireAccessible(field, "bean field");
                    // 没有 getter 的字段也按同一套实体元数据走，避免 Bean 和 record 表现不一致。
                    readers.put(persistentField.columnName(),
                                stored(new FieldValueReader(field), persistentField, field.getType()));
                }
            }
            return new EntityValues<>(readers, metadata, fieldFiller);
        } catch (IntrospectionException error) {
            throw new MappingException("entity values cannot be created for " + type.getName(), error);
        }
    }

    public int logicalWeight() {
        // 与元数据、行映射计划统一使用“反射槽”计量，避免固定头部被三种派生模型重复收费。
        return Math.max(1, readers.size());
    }

    private static List<Field> persistentFields(Class<?> type) {
        List<Class<?>> hierarchy = new ArrayList<>();
        for (Class<?> current = type; current != null && current != Object.class; current = current.getSuperclass()) {
            hierarchy.add(current);
        }
        Collections.reverse(hierarchy);
        List<Field> fields = new ArrayList<>();
        // 元数据已经决定哪些父类能持久化，这里只负责找到对应 Field；普通父类字段会在 findField 时被跳过。
        hierarchy.forEach(current -> Collections.addAll(fields, current.getDeclaredFields()));
        return fields;
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

        Object read(Object target);
    }

    private static ValueReader stored(ValueReader reader, EntityFieldMetadata field, Class<?> javaType) {
        ValueReader storedReader;
        String enumMember = field.enumValueMember();
        if (enumMember != null) {
            EntityEnumValueCodec codec = EntityEnumValueCodec.create(javaType, enumMember);
            storedReader = target -> codec.write(reader.read(target));
        } else {
            EntityEnumStorage storage = field.enumStorage();
            storedReader = storage == EntityEnumStorage.NONE
                    ? reader
                    : target -> {
                        Object value = reader.read(target);
                        if (!(value instanceof Enum<?> enumValue)) {
                            return value;
                        }
                        return storage == EntityEnumStorage.ORDINAL ? enumValue.ordinal() : enumValue.name();
                    };
        }
        if (!VARCHAR_TYPE.equals(field.databaseType())) {
            return storedReader;
        }
        // 这些标准 Java 类型由实体模型明确落为跨方言 VARCHAR；Repository 自动生成的主键/乐观锁条件
        // 会直接复用本 Map，因此必须在这里统一成与普通写入相同的数据库文本形态。
        return target -> textBackedValue(storedReader.read(target));
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

    private record MethodValueReader(Method method) implements ValueReader {

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

    private record FieldValueReader(Field field) implements ValueReader {

        @Override
        public Object read(Object target) {
            try {
                return field.get(target);
            } catch (IllegalAccessException error) {
                throw new MappingException("entity field cannot be read: " + field.getName(), error);
            }
        }
    }
}
