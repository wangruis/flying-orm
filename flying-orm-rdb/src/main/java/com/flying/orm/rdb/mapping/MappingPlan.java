package com.flying.orm.rdb.mapping;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.core.internal.error.ThrowableGraph;
import com.flying.orm.rdb.internal.mapping.EntityEnumValueCodec;
import com.flying.orm.rdb.internal.mapping.EntityFieldNames;
import com.flying.orm.rdb.internal.mapping.EntityMetadataResolver;
import com.flying.orm.rdb.result.DynamicRow;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 预先编译“数据库行怎样写成对象”的映射计划，真正映射每一行时不再扫描构造器、setter 和字段。
 *
 * <p>record 按规范构造器参数顺序一次性创建；普通 bean 优先调用 setter，没有 setter 时才直接写字段。
 * Java 属性名和数据库列名都归一化成同一个查找键，因此 {@code userName}、{@code user_name} 能映射到同一属性。</p>
 *
 * <p>计划由客户端实例持有的 {@link EntityModelRegistry} 按“实体类型 + codec 注册表实例”做有界权重缓存。
 * 静态便捷入口默认不缓存，避免全局长期持有应用类；MappingPlan 构造后只读，可在同一客户端的查询间共享。</p>
 *
 * @param <T> 目标对象类型
 * @author wangr
 * @date 2026-07-26
 * @version v1.0
 */
final class MappingPlan<T> implements RowMapper<T> {

    private final RecordWriter<T> recordWriter;
    private final BeanWriter<T> beanWriter;
    private final ValueCodecRegistry valueCodecs;

    private MappingPlan(RecordWriter<T> recordWriter,
                        BeanWriter<T> beanWriter,
                        ValueCodecRegistry valueCodecs) {
        this.recordWriter = recordWriter;
        this.beanWriter = beanWriter;
        this.valueCodecs = Objects.requireNonNull(valueCodecs, "value codec registry must not be null");
    }
    @SuppressWarnings("unchecked")
    static <T> MappingPlan<T> of(Class<T> type) {
        return of(type, ValueCodecRegistry.standard());
    }

    /**
     * 为目标类型和应用 codec 注册表创建或复用映射计划。
     *
     * <p>同一个实体可能被不同客户端使用不同转换规则，所以实例注册表的缓存键同时包含实体类型和注册表实例。
     * 此静态便捷入口直接编译一次；生产热路径应由客户端实例注册表复用计划。</p>
     *
     * @param type 目标对象类型
     * @param valueCodecs 应用级只读 codec 注册表
     * @param <T> 目标对象类型
     * @return 可并发复用的映射计划
     */
    static <T> MappingPlan<T> of(Class<T> type, ValueCodecRegistry valueCodecs) {
        Class<T> safeType = Objects.requireNonNull(type, "mapping type must not be null");
        return createUncached(safeType,
                              EntityMetadataResolver.createUncached(safeType),
                              Objects.requireNonNull(valueCodecs, "value codec registry must not be null"));
    }

    @Override
    public T map(DynamicRow row) {
        DynamicRow safeRow = Objects.requireNonNull(row, "row must not be null");
        BoundWriter<T> bound = safeRow.mappingBinding(this, () -> bind(safeRow));
        return bound.write(safeRow, valueCodecs);
    }

    private BoundWriter<T> bind(DynamicRow row) {
        if (row.hasAmbiguousMappingColumns()) {
            throw new MappingException("column names become ambiguous after normalization");
        }
        return recordWriter != null ? recordWriter.bind(row) : beanWriter.bind(row);
    }

    static <T> MappingPlan<T> createUncached(Class<T> type,
                                              EntityMetadata<T> metadata,
                                              ValueCodecRegistry valueCodecs) {
        if (type.isRecord()) {
            return new MappingPlan<>(recordWriter(type, metadata), null, valueCodecs);
        }
        return new MappingPlan<>(null, beanWriter(type, metadata), valueCodecs);
    }

    /** 映射计划的稳定逻辑重量，用反射写入槽数量近似长期占用，不在热路径扫描对象字节。 */
    int logicalWeight() {
        return recordWriter != null ? Math.max(1, recordWriter.names().length)
                : Math.max(1, beanWriter.writers().size());
    }

    private static <T> RecordWriter<T> recordWriter(Class<T> type, EntityMetadata<T> metadata) {
        try {
            RecordComponent[] components = type.getRecordComponents();
            Class<?>[] parameterTypes = Arrays.stream(components)
                                              .map(RecordComponent::getType)
                                              .toArray(Class<?>[]::new);
            // record 的组件顺序就是规范构造器顺序，不能按查询列顺序重排参数。
            Constructor<T> constructor = type.getDeclaredConstructor(parameterTypes);
            requireAccessible(constructor, "record constructor");
            String[] names = new String[components.length];
            DatabaseType[] databaseTypes = new DatabaseType[components.length];
            EntityEnumStorage[] enumStorage = new EntityEnumStorage[components.length];
            EntityEnumValueCodec[] enumValues = new EntityEnumValueCodec[components.length];
            Object[] defaultValues = new Object[components.length];
            for (int index = 0; index < components.length; index++) {
                RecordComponent component = components[index];
                EntityFieldMetadata field = metadata.findField(component.getName()).orElse(null);
                if (field == null) {
                    // exist=false 的组件不读取结果列，但仍必须占据规范构造器的参数位置。
                    defaultValues[index] = javaDefaultValue(component.getType());
                    continue;
                }
                names[index] = EntityFieldNames.resultKey(field.columnName());
                databaseTypes[index] = field.databaseType();
                enumStorage[index] = field.enumStorage();
                enumValues[index] = EntityRowValueConverter.enumValueCodec(component.getType(), field);
            }
            return new RecordWriter<>(constructor,
                                      names,
                                      parameterTypes,
                                      databaseTypes,
                                      enumStorage,
                                      enumValues,
                                      defaultValues);
        } catch (ReflectiveOperationException error) {
            throw new MappingException("record mapping plan cannot be created for " + type.getName(), error);
        }
    }

    /** 返回规范构造器所需的 Java 默认值；仅在创建映射计划时为非持久化组件计算一次。 */
    private static Object javaDefaultValue(Class<?> type) {
        return type.isPrimitive() ? Array.get(Array.newInstance(type, 1), 0) : null;
    }

    private static <T> BeanWriter<T> beanWriter(Class<T> type, EntityMetadata<T> metadata) {
        try {
            Constructor<T> constructor = type.getDeclaredConstructor();
            requireAccessible(constructor, "bean constructor");
            Map<String, EntityValueWriter> writers = new LinkedHashMap<>();
            // setter 优先，允许实体在赋值时保留自己的校验或派生逻辑。
            for (PropertyDescriptor property : Introspector.getBeanInfo(type).getPropertyDescriptors()) {
                Method writeMethod = property.getWriteMethod();
                EntityFieldMetadata field = metadata.findField(property.getName()).orElse(null);
                if (writeMethod != null && field != null && !"class".equals(property.getName())) {
                    requireAccessible(writeMethod, "bean setter");
                    EntityValueWriter writer = EntityValueWriter.forMethod(
                            writeMethod, writeMethod.getParameterTypes()[0], field);
                    // 行数据可能来自 select name，也可能来自真实列 user_name，两种都能写回 Java 属性。
                    writers.put(EntityFieldNames.resultKey(property.getName()), writer);
                    writers.put(EntityFieldNames.resultKey(metadata.field(property.getName()).columnName()), writer);
                }
            }
            // 只有没有 setter writer 的实例字段才使用直接反射写入，避免同一属性写两次。
            for (Field field : persistentFields(type)) {
                EntityFieldMetadata persistentField = metadata.findField(field.getName()).orElse(null);
                if (!Modifier.isStatic(field.getModifiers())
                        && persistentField != null
                        && !writers.containsKey(EntityFieldNames.resultKey(field.getName()))) {
                    requireAccessible(field, "bean field");
                    EntityValueWriter writer = EntityValueWriter.forField(
                            field, field.getType(), persistentField);
                    // 没有 setter 的字段同样同时认 Java 字段名和数据库列名。
                    writers.put(EntityFieldNames.resultKey(field.getName()), writer);
                    writers.put(EntityFieldNames.resultKey(metadata.field(field.getName()).columnName()), writer);
                }
            }
            return new BeanWriter<>(constructor, writers);
        } catch (ReflectiveOperationException | IntrospectionException error) {
            throw new MappingException("bean mapping plan cannot be created for " + type.getName(), error);
        }
    }

    private static List<Field> persistentFields(Class<?> type) {
        List<Class<?>> hierarchy = new ArrayList<>();
        for (Class<?> current = type; current != null && current != Object.class; current = current.getSuperclass()) {
            hierarchy.add(current);
        }
        Collections.reverse(hierarchy);
        List<Field> fields = new ArrayList<>();
        // 是否持久化由 EntityMetadata 决定；这里遍历完整继承链只是为了给没有 setter 的继承字段找到写入点。
        hierarchy.forEach(current -> Collections.addAll(fields, current.getDeclaredFields()));
        return fields;
    }

    /**
     * 提前验证反射成员是否真的能访问。Java 模块没有开放对应包时，{@code trySetAccessible()} 会返回
     * {@code false}；在这里给出稳定、带成员信息的映射错误，比等到逐行映射时冒出底层反射异常更容易定位。
     */
    private static void requireAccessible(AccessibleObject member, String role) {
        if (!member.trySetAccessible()) {
            throw new MappingException(role + " is not accessible; open the entity package or expose a public member: "
                                               + member);
        }
    }

    private record RecordWriter<T>(Constructor<T> constructor,
                                   String[] names,
                                   Class<?>[] parameterTypes,
                                   DatabaseType[] databaseTypes,
                                   EntityEnumStorage[] enumStorage,
                                   EntityEnumValueCodec[] enumValues,
                                   Object[] defaultValues) {

        private BoundWriter<T> bind(DynamicRow row) {
            int[] indexes = new int[names.length];
            for (int index = 0; index < names.length; index++) {
                indexes[index] = names[index] == null ? -1 : row.mappingIndexOf(names[index]);
            }
            return new BoundRecordWriter<>(this, indexes);
        }
    }

    private record BoundRecordWriter<T>(RecordWriter<T> writer,
                                        int[] indexes) implements BoundWriter<T> {

        @Override
        public T write(DynamicRow row, ValueCodecRegistry valueCodecs) {
            String[] names = writer.names();
            Object[] arguments = new Object[names.length];
            for (int i = 0; i < names.length; i++) {
                if (names[i] == null) {
                    arguments[i] = writer.defaultValues()[i];
                    continue;
                }
                // 持久化组件即使查询缺列也保持参数位置，缺失值由目标类型的统一转换规则处理。
                arguments[i] = EntityRowValueConverter.convert(
                        indexes[i] < 0 ? null : row.value(indexes[i]),
                        writer.parameterTypes()[i], writer.databaseTypes()[i],
                        writer.enumStorage()[i], writer.enumValues()[i], valueCodecs);
            }
            try {
                return writer.constructor().newInstance(arguments);
            } catch (ReflectiveOperationException error) {
                ThrowableGraph.rethrowVirtualMachineError(error);
                throw new MappingException("record row cannot be mapped", error);
            }
        }
    }

    private record BeanWriter<T>(Constructor<T> constructor, Map<String, EntityValueWriter> writers) {

        private BoundWriter<T> bind(DynamicRow row) {
            int[] indexes = new int[row.columnCount()];
            EntityValueWriter[] boundWriters = new EntityValueWriter[row.columnCount()];
            int size = 0;
            for (int index = 0; index < row.columnCount(); index++) {
                EntityValueWriter writer = writers.get(row.mappingKey(index));
                if (writer != null) {
                    indexes[size] = index;
                    boundWriters[size++] = writer;
                }
            }
            return new BoundBeanWriter<>(this,
                                         Arrays.copyOf(indexes, size),
                                         Arrays.copyOf(boundWriters, size));
        }
    }

    private record BoundBeanWriter<T>(BeanWriter<T> writer,
                                      int[] indexes,
                                      EntityValueWriter[] writers) implements BoundWriter<T> {

        @Override
        public T write(DynamicRow row, ValueCodecRegistry valueCodecs) {
            try {
                T target = writer.constructor().newInstance();
                // 查询结果里的额外列直接忽略，支持 join/计算列与窄实体并存。
                for (int index = 0; index < indexes.length; index++) {
                    writers[index].write(target, row.value(indexes[index]), valueCodecs);
                }
                return target;
            } catch (ReflectiveOperationException error) {
                ThrowableGraph.rethrowVirtualMachineError(error);
                throw new MappingException("bean row cannot be mapped", error);
            }
        }
    }

    @FunctionalInterface
    private interface BoundWriter<T> {

        T write(DynamicRow row, ValueCodecRegistry valueCodecs);
    }

}
