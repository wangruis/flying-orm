package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.internal.error.ThrowableGraph;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.id.IdGenerator;
import com.flying.orm.rdb.internal.mapping.EntityFieldNames;
import com.flying.orm.rdb.internal.mapping.EntityMetadataHierarchy;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.result.DynamicRow;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * 统一处理实体主键的插入前校验、应用侧 UUID 生成和数据库生成键回填。
 *
 * <p>反射成员只在 Repository 创建时解析一次，单次 insert 只做直接调用。AUTO 主键必须为空并交给数据库；
 * NONE/INPUT 必须由调用方提供。ASSIGN_ID 只调用客户端显式配置的生成器；没有配置可靠节点生成器时会在 SQL 前失败，
 * 绝不会为了“能运行”而偷偷使用一个可能重复的默认节点。</p>
 */
final class RepositoryEntityIdSupport<T> {

    private final List<PrimaryKey> primaryKeys;
    private final PrimaryKey generatedKey;
    private final IdGenerator generator;
    private final EntityTypeMappingRegistry.Mapping generatedKeyMapping;

    private RepositoryEntityIdSupport(List<PrimaryKey> primaryKeys,
                                      PrimaryKey generatedKey,
                                      IdGenerator generator,
                                      EntityTypeMappingRegistry.Mapping generatedKeyMapping) {
        this.primaryKeys = primaryKeys;
        this.generatedKey = generatedKey;
        this.generator = generator;
        this.generatedKeyMapping = generatedKeyMapping;
    }

    static <T> RepositoryEntityIdSupport<T> create(EntityMetadata<T> metadata, IdGenerator generator) {
        return create(metadata, generator, null);
    }

    /** Repository 单条与批量入口共用同一份启动期生成键映射。 */
    static <T> RepositoryEntityIdSupport<T> create(
            EntityMetadata<T> metadata,
            EntityModelRegistry models) {
        EntityModelRegistry safeModels = Objects.requireNonNull(
                models, "entity model registry must not be null");
        EntityMetadata<T> safeMetadata = Objects.requireNonNull(
                metadata, "entity metadata must not be null");
        return create(
                safeMetadata,
                safeModels.idGenerator(),
                safeModels.databaseGeneratedKeyMapping(safeMetadata.type()));
    }

    /**
     * 为已经注册完整关系描述的实体缓存数据库生成键 codec；未注册自定义映射时使用标准主键转换。
     * 映射只在 Repository 创建时解析并传入，逐次回填不再查注册表。
     */
    static <T> RepositoryEntityIdSupport<T> create(
            EntityMetadata<T> metadata,
            IdGenerator generator,
            EntityTypeMappingRegistry.Mapping generatedKeyMapping) {
        EntityMetadata<T> safeMetadata = Objects.requireNonNull(metadata, "entity metadata must not be null");
        IdGenerator safeGenerator = Objects.requireNonNull(generator, "id generator must not be null");
        List<PrimaryKey> primaryKeys = safeMetadata.primaryKeyFields().stream()
                .map(field -> new PrimaryKey(field, resolveAccess(safeMetadata.type(), field)))
                .toList();
        List<PrimaryKey> generatedKeys = primaryKeys.stream()
                .filter(key -> key.field().generation().generated())
                .toList();
        if (generatedKeys.size() > 1) {
            throw new MappingException("entity cannot declare more than one database-generated primary key");
        }
        return new RepositoryEntityIdSupport<>(
                primaryKeys,
                generatedKeys.isEmpty() ? null : generatedKeys.getFirst(),
                safeGenerator,
                generatedKeyMapping);
    }

    /** SQL 生成前完成全部本地主键规则，失败时数据库还没有发生任何写入。 */
    void prepare(T entity) {
        prepare(entity, false);
    }

    void prepare(T entity, boolean preserveGeneratedKey) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        List<Assignment> assignments = List.of();
        for (PrimaryKey primaryKey : primaryKeys) {
            Assignment assignment = prepare(safeEntity, primaryKey, preserveGeneratedKey);
            if (assignment != null) {
                if (assignments.isEmpty()) assignments = new ArrayList<>();
                assignments.add(assignment);
            }
        }
        if (!assignments.isEmpty()) {
            assignments.forEach(assignment -> assignment.access().write(safeEntity, assignment.value()));
        }
    }

    private Assignment prepare(T entity, PrimaryKey primaryKey, boolean preserveGeneratedKey) {
        EntityFieldMetadata id = primaryKey.field();
        ValueAccess access = primaryKey.access();
        Object current = access.read(entity);
        if (id.generation().generated()) {
            if (current != null && !preserveGeneratedKey) throw new MappingException("database-generated primary key must be null before insert");
            return null;
        }
        return switch (id.idType()) {
            case AUTO -> throw new MappingException("AUTO primary key is missing database generation metadata");
            case NONE, INPUT -> {
                if (current == null) throw new MappingException("INPUT primary key must be assigned before insert");
                yield null;
            }
            case ASSIGN_UUID -> current == null ? new Assignment(access, uuidValue(access.valueType())) : null;
            case ASSIGN_ID -> {
                if (current != null) {
                    yield null;
                }
                try {
                    Object generated = generator.generate(entity.getClass(), id.name(), access.valueType());
                    yield new Assignment(access, convert(generated, access.valueType()));
                } catch (RuntimeException error) {
                    throw new MappingException("ASSIGN_ID generation failed: " + id.name(), error);
                }
            }
        };
    }

    boolean databaseGenerated() {
        return generatedKey != null;
    }

    boolean requiresGeneratedKey(T entity) {
        return generatedKey != null && generatedKey.access().read(entity) == null;
    }

    /**
     * @return 执行内核请求生成键时使用的真实数据库列名
     * @throws IllegalStateException 当前实体没有数据库生成主键
     */
    String generatedKeyColumn() {
        if (!databaseGenerated()) {
            throw new IllegalStateException("entity does not use a database-generated primary key");
        }
        return generatedKey.field().columnName();
    }

    /** 单条回填失败按 ASSIGN 阶段报告，影响行数保持数据库已经报告的实际值。 */
    long assignGeneratedKey(T entity, SqlWriteResult result) {
        try {
            applyGeneratedKey(entity, result);
            return result.affectedRows();
        } catch (RuntimeException failure) {
            Throwable preferred = RepositoryFailureSupport.preferVirtualMachineError(failure);
            if (preferred instanceof VirtualMachineError fatal) {
                throw fatal;
            }
            throw new GeneratedKeyResolutionException(
                    result.affectedRows(), GeneratedKeyResolutionException.Phase.ASSIGN, failure);
        }
    }

    /** 把驱动返回的唯一键写回原实体；缺失或多行结果都拒绝，不能猜测实体对应哪一行。 */
    void applyGeneratedKey(T entity, SqlWriteResult result) {
        if (!databaseGenerated()) {
            return;
        }
        SqlWriteResult safeResult = Objects.requireNonNull(result, "generated key result must not be null");
        if (safeResult.generatedKeys().size() != 1) {
            throw new MappingException("database must return exactly one generated primary key");
        }
        applyGeneratedKey(entity, safeResult.generatedKeys().getFirst());
    }

    /**
     * 按批量输入偏移收到一行生成键后复用单条 insert 的列匹配和类型转换规则。
     */
    void applyGeneratedKey(T entity, DynamicRow generatedKey) {
        if (!databaseGenerated()) {
            throw new MappingException("entity does not declare a database-generated primary key");
        }
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        DynamicRow safeKey = Objects.requireNonNull(generatedKey, "generated key row must not be null");
        Object value = keyValue(safeKey);
        ValueAccess access = this.generatedKey.access();
        Object converted = generatedKeyValue(value, access.valueType());
        access.write(safeEntity, converted);
    }

    private Object generatedKeyValue(Object value, Class<?> type) {
        if (generatedKeyMapping == null) {
            return convert(value, type);
        }
        if (value == null) {
            throw new MappingException("database returned a null generated primary key");
        }
        try {
            Object decoded = generatedKeyMapping.codec().read(value, type);
            if (decoded == null) {
                throw new MappingException("database returned a null generated primary key");
            }
            return decoded;
        } catch (IllegalArgumentException error) {
            throw new MappingException("generated primary key cannot be converted to " + type.getName(), error);
        }
    }

    private Object keyValue(DynamicRow row) {
        // 单列结果没有歧义，也兼容驱动的 GENERATED_KEY 等标签。
        if (row.columnCount() == 1) return row.value(0);
        EntityFieldMetadata id = generatedKey.field();
        String expected = EntityFieldNames.key(id.columnName());
        Object matched = null;
        int matches = 0;
        for (int index = 0; index < row.columnCount(); index++) {
            if (EntityFieldNames.key(row.columnName(index)).equals(expected)) {
                matched = row.value(index);
                matches++;
            }
        }
        if (matches == 1) {
            return matched;
        }
        throw new MappingException("generated primary key column is missing or ambiguous: " + id.columnName());
    }

    private static ValueAccess resolveAccess(Class<?> type, EntityFieldMetadata id) {
        Method reader = null;
        Method writer = null;
        try {
            if (type.isRecord()) {
                reader = accessible(type.getDeclaredMethod(id.name()), id.name());
            } else {
                for (PropertyDescriptor property : Introspector.getBeanInfo(type).getPropertyDescriptors()) {
                    if (EntityFieldNames.matches(property.getName(), id.name())) {
                        Method read = accessible(property.getReadMethod(), property.getName());
                        Method write = accessible(property.getWriteMethod(), property.getName());
                        // 与实体读写计划一致：后出现的有效访问器覆盖同名别名，缺失或排除的访问器不覆盖。
                        if (read != null) reader = read;
                        if (write != null) writer = write;
                    }
                }
            }
        } catch (IntrospectionException | NoSuchMethodException error) {
            throw new MappingException("primary key property cannot be inspected: " + id.name(), error);
        }
        Field field = findField(type, id.name());
        if (reader == null && field == null) {
            throw new MappingException("primary key member cannot be read: " + id.name());
        }
        boolean mayWrite = id.generation().generated()
                || id.idType() == IdType.ASSIGN_ID
                || id.idType() == IdType.ASSIGN_UUID;
        if (mayWrite && writer == null && (field == null || Modifier.isFinal(field.getModifiers()))) {
            throw new MappingException("generated or assigned primary key needs a mutable bean property: " + id.name());
        }
        return new ValueAccess(reader, writer, field, EntityMetadataHierarchy.memberClass(type,
                reader != null ? reader.getGenericReturnType() : field.getGenericType()));
    }

    private static Field findField(Class<?> type, String name) {
        for (Class<?> current = type; current != null && current != Object.class; current = current.getSuperclass()) {
            try {
                Field field = current.getDeclaredField(name);
                if (!EntityMetadataHierarchy.isPersistentField(field)) {
                    // 非持久子字段不能遮住真正的继承主键；回填必须使用持久成员。
                    continue;
                }
                if (!field.trySetAccessible()) {
                    throw new MappingException("primary key field is not accessible: " + field);
                }
                return field;
            } catch (NoSuchFieldException ignored) {
                // 继续到父类查找继承主键。
            }
        }
        return null;
    }

    private static Method accessible(Method method, String propertyName) {
        if (method == null || !EntityMetadataHierarchy.isPersistentAccessor(method, propertyName)) {
            return null;
        }
        if (!method.trySetAccessible()) {
            throw new MappingException("primary key method is not accessible: " + method);
        }
        return method;
    }

    private static Object uuidValue(Class<?> type) {
        UUID uuid = UUID.randomUUID();
        if (type == UUID.class) {
            return uuid;
        }
        if (type == String.class) {
            return uuid.toString().replace("-", "");
        }
        throw new MappingException("ASSIGN_UUID only supports String or UUID primary keys: " + type.getName());
    }

    private static Object convert(Object value, Class<?> type) {
        if (value == null) {
            throw new MappingException("database returned a null generated primary key");
        }
        if (type.isInstance(value)) {
            return value;
        }
        if (type == String.class) {
            return value.toString();
        }
        if (value instanceof Number number) {
            try {
                BigDecimal decimal = new BigDecimal(number.toString());
                if (type == Long.class || type == long.class) return decimal.longValueExact();
                if (type == Integer.class || type == int.class) return decimal.intValueExact();
                if (type == Short.class || type == short.class) return decimal.shortValueExact();
                if (type == Byte.class || type == byte.class) return decimal.byteValueExact();
                if (type == BigInteger.class) return decimal.toBigIntegerExact();
                if (type == BigDecimal.class) return decimal;
            } catch (ArithmeticException error) {
                throw new MappingException("generated primary key cannot be converted to " + type.getName(), error);
            }
        }
        if (type == UUID.class) {
            String text = value.toString();
            try {
                return UUID.fromString(text);
            } catch (IllegalArgumentException ignored) {
                // UUID 解析异常会把原始数据库值写入 message；映射边界只暴露稳定分类，避免日志泄露或放大。
                throw new MappingException("generated primary key cannot be converted to " + type.getName());
            }
        }
        throw new MappingException("generated primary key cannot be converted to " + type.getName());
    }


    /** Repository 创建时解析一次的主键字段与访问器。 */
    private record PrimaryKey(EntityFieldMetadata field, ValueAccess access) {
    }

    /** 全部主键规则验证完成后才应用的本地生成值。 */
    private record Assignment(ValueAccess access, Object value) {
    }

    /** getter/setter 优先，缺少访问器时使用已经提前开放的字段。 */
    private record ValueAccess(Method reader, Method writer, Field field, Class<?> valueType) {

        private Object read(Object entity) {
            try {
                return reader != null ? reader.invoke(entity) : field.get(entity);
            } catch (ReflectiveOperationException error) {
                ThrowableGraph.rethrowVirtualMachineError(error);
                throw new MappingException("primary key cannot be read", error);
            }
        }

        private void write(Object entity, Object value) {
            try {
                if (writer != null) writer.invoke(entity, value); else field.set(entity, value);
            } catch (ReflectiveOperationException error) {
                ThrowableGraph.rethrowVirtualMachineError(error);
                throw new MappingException("primary key cannot be written", error);
            }
        }
    }
}
