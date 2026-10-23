package com.flying.orm.rdb.mapping;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.internal.error.ThrowableGraph;
import com.flying.orm.rdb.internal.mapping.EntityFieldNames;
import com.flying.orm.rdb.internal.mapping.EntityMetadataHierarchy;
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
import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;

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
    static <T> MappingPlan<T> of(Class<T> type) {
        return of(type, ValueCodecRegistry.standard());
    }

    /** 为类型和 codec 注册表编译独立计划；跨查询复用由客户端 EntityModelRegistry 管理。 */
    static <T> MappingPlan<T> of(Class<T> type, ValueCodecRegistry valueCodecs) {
        Class<T> safeType = Objects.requireNonNull(type, "mapping type must not be null");
        return createUncached(safeType,
                              EntityMetadataResolver.createUncached(safeType),
                              Objects.requireNonNull(valueCodecs, "value codec registry must not be null"));
    }

    static <T> RowMapper<T> nested(Class<T> type, ValueCodecRegistry valueCodecs) {
        MappingPlan<T> root = of(type, valueCodecs);
        return new RowMapper<>() {
            private final ClassValue<MappingPlan<?>> plans = new ClassValue<>() {
                @Override
                protected MappingPlan<?> computeValue(Class<?> nestedType) {
                    return nestedType == type ? root : of(nestedType, valueCodecs);
                }
            };

            @Override
            public T map(DynamicRow row) {
                DynamicRow source = Objects.requireNonNull(row, "row must not be null");
                NestedBinding<T> binding = source.mappingBinding(this,
                        () -> root.bindNested(NestedColumns.of(source), List.of(""), plans));
                return binding.writer().write(source, valueCodecs);
            }

            @Override
            public Function<IntFunction<Object>, T> bindColumns(DynamicRow columns) {
                NestedBinding<T> binding = root.bindNested(NestedColumns.of(columns), List.of(""), plans);
                return values -> binding.writer().write(values, valueCodecs);
            }
        };
    }

    static RowMapper<Map<String, Object>> nestedMap() {
        return new RowMapper<>() {
            @Override
            public Map<String, Object> map(DynamicRow row) {
                return row.mappingBinding(this, () -> bindColumns(row)).apply(row);
            }

            @Override
            public Function<IntFunction<Object>, Map<String, Object>> bindColumns(DynamicRow columns) {
                List<MapPath> paths = new ArrayList<>();
                Map<String, Integer> branches = new LinkedHashMap<>();
                Set<String> leaves = new LinkedHashSet<>();
                for (int column = 0; column < columns.columnCount(); column++) {
                    String[] parts = columns.columnName(column).split("\\.", -1);
                    StringBuilder path = new StringBuilder();
                    int parent = 0;
                    for (int part = 0; part < parts.length; part++) {
                        if (parts[part].isEmpty()) throw new MappingException("nested map path must not contain empty parts");
                        if (part > 0) path.append('.');
                        path.append(parts[part]);
                        String key = path.toString();
                        boolean leaf = part == parts.length - 1;
                        if (leaves.contains(key) || leaf && branches.containsKey(key)) {
                            throw new MappingException("nested map path has both a value column and descendant columns");
                        }
                        if (leaf) {
                            leaves.add(key);
                            paths.add(new MapPath(parent, parts[part], column));
                        } else {
                            Integer child = branches.get(key);
                            if (child == null) {
                                child = branches.size() + 1;
                                branches.put(key, child);
                                paths.add(new MapPath(parent, parts[part], -1));
                            }
                            parent = child;
                        }
                    }
                }
                int mapCount = branches.size() + 1;
                return values -> {
                    List<Map<String, Object>> maps = new ArrayList<>(mapCount);
                    maps.add(new LinkedHashMap<>());
                    for (MapPath path : paths) {
                        Object value;
                        if (path.column() < 0) {
                            Map<String, Object> child = new LinkedHashMap<>();
                            maps.add(child);
                            value = child;
                        } else {
                            value = values.apply(path.column());
                        }
                        maps.get(path.parent()).put(path.name(), value);
                    }
                    return maps.getFirst();
                };
            }
        };
    }

    private record MapPath(int parent, String name, int column) { }

    private NestedBinding<T> bindNested(NestedColumns columns, List<String> prefixes,
                                        ClassValue<MappingPlan<?>> plans) {
        NestedValue[] values;
        BoundWriter<T> bound;
        if (recordWriter != null) {
            values = new NestedValue[recordWriter.names().length];
            for (int index = 0; index < values.length; index++) {
                if (recordWriter.names()[index] != null) {
                    values[index] = nestedValue(columns, prefixes, recordWriter.names()[index],
                            recordWriter.propertyNames()[index], recordWriter.parameterTypes()[index], plans);
                }
            }
            bound = (row, codecs) -> {
                Object[] arguments = recordWriter.defaultValues().clone();
                for (int index = 0; index < values.length; index++) {
                    if (values[index] != null) {
                        arguments[index] = recordWriter.readers()[index].read(values[index].read(row, codecs), codecs);
                    }
                }
                return recordWriter.instantiate(arguments);
            };
        } else {
            List<EntityValueWriter> writers = new ArrayList<>();
            List<NestedValue> mapped = new ArrayList<>();
            for (BeanFieldWriter field : beanWriter.fields()) {
                NestedValue value = nestedValue(columns, prefixes, field.columnKey(), field.propertyKey(),
                        field.type(), plans);
                if (value != null) {
                    writers.add(field.writer());
                    mapped.add(value);
                }
            }
            values = mapped.toArray(NestedValue[]::new);
            bound = (row, codecs) -> {
                try {
                    T target = beanWriter.constructor().newInstance();
                    for (int index = 0; index < values.length; index++) {
                        writers.get(index).write(target, values[index].read(row, codecs), codecs);
                    }
                    return target;
                } catch (ReflectiveOperationException error) {
                    ThrowableGraph.rethrowVirtualMachineError(error);
                    throw new MappingException("bean row cannot be mapped", error);
                }
            };
        }
        return new NestedBinding<>(bound, values);
    }

    private static NestedValue nestedValue(NestedColumns columns, List<String> prefixes, String column,
                                           String property, Class<?> type, ClassValue<MappingPlan<?>> plans) {
        Set<String> paths = new LinkedHashSet<>();
        for (String prefix : prefixes) {
            paths.add(prefix + column);
            paths.add(prefix + property);
        }
        Integer index = null;
        List<String> childPrefixes = new ArrayList<>();
        for (String path : paths) {
            if (index == null) {
                index = columns.indexes().get(path);
            }
            if (columns.prefixes().contains(path + ".")) {
                childPrefixes.add(path + ".");
            }
        }
        if (index != null) {
            if (!childPrefixes.isEmpty()) {
                throw new MappingException("nested property has both a value column and descendant columns");
            }
            return new NestedValue(index, null);
        }
        if (childPrefixes.isEmpty()) {
            return null;
        }
        NestedBinding<?> child = plans.get(type).bindNested(columns, childPrefixes, plans);
        if (child.isEmpty()) {
            return null;
        }
        return new NestedValue(-1, child);
    }

    private record NestedValue(int index, NestedBinding<?> child) {
        private Object read(IntFunction<Object> row, ValueCodecRegistry codecs) {
            return child == null ? row.apply(index) : child.readNullable(row, codecs);
        }

        private boolean hasValue(IntFunction<Object> row) {
            return child == null ? row.apply(index) != null : child.hasValue(row);
        }
    }

    private record NestedBinding<T>(BoundWriter<T> writer, NestedValue[] values) {
        private boolean isEmpty() {
            for (NestedValue value : values) {
                if (value != null) return false;
            }
            return true;
        }

        private boolean hasValue(IntFunction<Object> row) {
            for (NestedValue value : values) {
                if (value != null && value.hasValue(row)) return true;
            }
            return false;
        }

        private T readNullable(IntFunction<Object> row, ValueCodecRegistry codecs) {
            return hasValue(row) ? writer.write(row, codecs) : null;
        }
    }

    private record NestedColumns(Map<String, Integer> indexes, Set<String> prefixes) {
        private static NestedColumns of(DynamicRow row) {
            Map<String, Integer> indexes = new LinkedHashMap<>();
            Set<String> prefixes = new LinkedHashSet<>();
            for (int index = 0; index < row.columnCount(); index++) {
                String[] parts = row.columnName(index).split("\\.", -1);
                StringBuilder path = new StringBuilder();
                for (String part : parts) {
                    if (!path.isEmpty()) {
                        path.append('.');
                        prefixes.add(path.toString());
                    }
                    path.append(EntityFieldNames.resultKey(part));
                }
                if (indexes.putIfAbsent(path.toString(), index) != null) {
                    throw new MappingException("column paths become ambiguous after normalization");
                }
            }
            return new NestedColumns(indexes, prefixes);
        }
    }

    @Override
    public T map(DynamicRow row) {
        DynamicRow safeRow = Objects.requireNonNull(row, "row must not be null");
        BoundWriter<T> bound = safeRow.mappingBinding(this, () -> bind(safeRow));
        return bound.write(safeRow, valueCodecs);
    }

    @Override
    public Function<IntFunction<Object>, T> bindColumns(DynamicRow columns) {
        BoundWriter<T> bound = bind(columns);
        return values -> bound.write(values, valueCodecs);
    }

    private BoundWriter<T> bind(DynamicRow row) {
        if (row.hasAmbiguousMappingColumns()) {
            throw new MappingException("column names become ambiguous after normalization");
        }
        if (recordWriter == null) {
            return beanWriter.bind(row);
        }
        return recordWriter.bind(row);
    }

    static <T> MappingPlan<T> createUncached(Class<T> type,
                                              EntityMetadata<T> metadata,
                                              ValueCodecRegistry valueCodecs) {
        return createUncached(type, metadata, valueCodecs, false, false);
    }

    static <T> MappingPlan<T> createUncached(
            Class<T> type, EntityMetadata<T> metadata, ValueCodecRegistry valueCodecs,
            boolean rawInput, boolean decodedInput) {
        if (type.isRecord()) {
            return new MappingPlan<>(recordWriter(type, metadata, rawInput, decodedInput), null, valueCodecs);
        }
        return new MappingPlan<>(null, beanWriter(type, metadata, rawInput, decodedInput), valueCodecs);
    }

    /** 映射计划的稳定逻辑重量，用反射写入槽数量近似长期占用，不在热路径扫描对象字节。 */
    int logicalWeight() {
        return recordWriter != null ? Math.max(1, recordWriter.names().length)
                : Math.max(1, beanWriter.writers().size());
    }

    private static <T> RecordWriter<T> recordWriter(
            Class<T> type,
            EntityMetadata<T> metadata,
            boolean rawInput,
            boolean decodedInput) {
        try {
            RecordComponent[] components = type.getRecordComponents();
            Class<?>[] parameterTypes = Arrays.stream(components)
                                              .map(RecordComponent::getType)
                                              .toArray(Class<?>[]::new);
            // record 的组件顺序就是规范构造器顺序，不能按查询列顺序重排参数。
            Constructor<T> constructor = type.getDeclaredConstructor(parameterTypes);
            requireAccessible(constructor, "record constructor");
            String[] names = new String[components.length];
            String[] propertyNames = new String[components.length];
            EntityRowValueConverter.Reader[] readers = new EntityRowValueConverter.Reader[components.length];
            Object[] defaultValues = new Object[components.length];
            for (int index = 0; index < components.length; index++) {
                RecordComponent component = components[index];
                defaultValues[index] = javaDefaultValue(component.getType());
                // findField 同时支持列别名；反射成员只能按真实 Java 属性名选择持久化元数据。
                EntityFieldMetadata field = metadata.findField(component.getName()).orElse(null);
                if (field == null || !field.name().equals(component.getName())) {
                    // exist=false 的组件不读取结果列，但仍必须占据规范构造器的参数位置。
                    continue;
                }
                names[index] = EntityFieldNames.resultKey(field.columnName());
                propertyNames[index] = EntityFieldNames.resultKey(field.name());
                readers[index] = EntityRowValueConverter.bind(type, type, component.getGenericType(),
                        component.getType(), field, rawInput, decodedInput);
            }
            return new RecordWriter<>(constructor,
                                      names,
                                      propertyNames,
                                      parameterTypes,
                                      readers,
                                      defaultValues);
        } catch (ReflectiveOperationException error) {
            throw new MappingException("record mapping plan cannot be created for " + type.getName(), error);
        }
    }

    /** 返回规范构造器所需的 Java 默认值；仅在创建映射计划时为非持久化组件计算一次。 */
    private static Object javaDefaultValue(Class<?> type) {
        return type.isPrimitive() ? Array.get(Array.newInstance(type, 1), 0) : null;
    }

    private static <T> BeanWriter<T> beanWriter(
            Class<T> type,
            EntityMetadata<T> metadata,
            boolean rawInput,
            boolean decodedInput) {
        try {
            Constructor<T> constructor = type.getDeclaredConstructor();
            requireAccessible(constructor, "bean constructor");
            Map<String, BeanFieldWriter> writers = new LinkedHashMap<>();
            // setter 优先，允许实体在赋值时保留自己的校验或派生逻辑。
            for (PropertyDescriptor property : Introspector.getBeanInfo(type).getPropertyDescriptors()) {
                Method writeMethod = property.getWriteMethod();
                EntityFieldMetadata field = metadata.findField(property.getName()).orElse(null);
                if (writeMethod != null && field != null
                        && EntityFieldNames.matches(field.name(), property.getName())
                        && EntityMetadataHierarchy.isPersistentAccessor(writeMethod, property.getName())
                        && !"class".equals(property.getName())) {
                    requireAccessible(writeMethod, "bean setter");
                    Class<?> memberType = EntityMetadataHierarchy.memberClass(type, writeMethod.getGenericParameterTypes()[0]);
                    EntityValueWriter writer = EntityValueWriter.forMethod(
                            writeMethod, EntityRowValueConverter.bind(type, writeMethod.getDeclaringClass(),
                                    writeMethod.getGenericParameterTypes()[0], memberType,
                                    field, rawInput, decodedInput));
                    // 行数据可能来自 select name，也可能来自真实列 user_name，两种都能写回 Java 属性。
                    registerWriter(writers, field, memberType, writer);
                }
            }
            // 只有没有 setter writer 的实例字段才使用直接反射写入，避免同一属性写两次。
            for (Field field : EntityMetadataHierarchy.persistentFields(type)) {
                EntityFieldMetadata persistentField = metadata.findField(field.getName()).orElse(null);
                if (persistentField != null
                        && persistentField.name().equals(field.getName())
                        && !writers.containsKey(EntityFieldNames.resultKey(field.getName()))) {
                    requireAccessible(field, "bean field");
                    Class<?> memberType = EntityMetadataHierarchy.memberClass(type, field.getGenericType());
                    EntityValueWriter writer = EntityValueWriter.forField(
                            field, EntityRowValueConverter.bind(type, field.getDeclaringClass(), field.getGenericType(),
                                    memberType, persistentField, rawInput, decodedInput));
                    // 没有 setter 的字段同样同时认 Java 字段名和数据库列名。
                    registerWriter(writers, persistentField, memberType, writer);
                }
            }
            return new BeanWriter<>(constructor, writers, List.copyOf(new LinkedHashSet<>(writers.values())));
        } catch (ReflectiveOperationException | IntrospectionException error) {
            throw new MappingException("bean mapping plan cannot be created for " + type.getName(), error);
        }
    }

    private static void registerWriter(Map<String, BeanFieldWriter> writers,
                                       EntityFieldMetadata field,
                                       Class<?> type,
                                       EntityValueWriter writer) {
        String columnKey = EntityFieldNames.resultKey(field.columnName());
        String propertyKey = EntityFieldNames.resultKey(field.name());
        BeanFieldWriter binding = new BeanFieldWriter(columnKey, propertyKey, type, writer);
        writers.put(propertyKey, binding);
        writers.put(columnKey, binding);
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
                                   String[] propertyNames,
                                   Class<?>[] parameterTypes,
                                   EntityRowValueConverter.Reader[] readers,
                                   Object[] defaultValues) {

        private BoundRecordWriter<T> bind(DynamicRow row) {
            int[] indexes = new int[names.length];
            for (int index = 0; index < names.length; index++) {
                indexes[index] = names[index] == null ? -1 : row.mappingIndexOf(names[index]);
                // 物理列保持原优先级；仅缺列时尝试 Java 属性标签，逐行写入仍只读已绑定索引。
                if (indexes[index] < 0 && names[index] != null) {
                    indexes[index] = row.mappingIndexOf(propertyNames[index]);
                }
            }
            return new BoundRecordWriter<>(this, indexes);
        }

        private T instantiate(Object[] arguments) {
            try {
                return constructor.newInstance(arguments);
            } catch (ReflectiveOperationException error) {
                ThrowableGraph.rethrowVirtualMachineError(error);
                throw new MappingException("record row cannot be mapped", error);
            }
        }
    }

    private record BoundRecordWriter<T>(RecordWriter<T> writer,
                                        int[] indexes) implements BoundWriter<T> {

        @Override
        public T write(IntFunction<Object> row, ValueCodecRegistry valueCodecs) {
            String[] names = writer.names();
            Object[] arguments = new Object[names.length];
            for (int i = 0; i < names.length; i++) {
                if (names[i] == null || indexes[i] < 0) {
                    arguments[i] = writer.defaultValues()[i];
                    continue;
                }
                arguments[i] = writer.readers()[i].read(row.apply(indexes[i]), valueCodecs);
            }
            return instantiate(arguments);
        }

        private T instantiate(Object[] arguments) {
            return writer.instantiate(arguments);
        }
    }

    private record BeanFieldWriter(String columnKey, String propertyKey, Class<?> type, EntityValueWriter writer) {
    }

    private record BeanWriter<T>(Constructor<T> constructor, Map<String, BeanFieldWriter> writers,
                                 List<BeanFieldWriter> fields) {

        private BoundWriter<T> bind(DynamicRow row) {
            int[] indexes = new int[row.columnCount()];
            EntityValueWriter[] boundWriters = new EntityValueWriter[row.columnCount()];
            int size = 0;
            for (int index = 0; index < row.columnCount(); index++) {
                BeanFieldWriter field = writers.get(row.mappingKey(index));
                if (field != null) {
                    // 与 record 一致：物理列存在时只绑定它，SQL NULL 不触发属性标签回退。
                    int physicalIndex = row.mappingIndexOf(field.columnKey());
                    if (physicalIndex >= 0 && physicalIndex != index) {
                        continue;
                    }
                    indexes[size] = index;
                    boundWriters[size++] = field.writer();
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
        public T write(IntFunction<Object> row, ValueCodecRegistry valueCodecs) {
            try {
                T target = writer.constructor().newInstance();
                // 查询结果里的额外列直接忽略，支持 join/计算列与窄实体并存。
                for (int index = 0; index < indexes.length; index++) {
                    writers[index].write(target, row.apply(indexes[index]), valueCodecs);
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

        T write(IntFunction<Object> row, ValueCodecRegistry valueCodecs);
    }

}
