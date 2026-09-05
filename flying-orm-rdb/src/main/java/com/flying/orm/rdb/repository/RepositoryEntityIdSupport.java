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
     * 为已经注册完整关系描述的实体缓存数据库生成键 codec；普通 CRUD 继续使用旧入口和原转换规则。
     * 映射只在 Repository 创建时解析并传入，逐次回填不再查注册表。
     */
    static <T> RepositoryEntityIdSupport<T> create(
            EntityMetadata<T> metadata,
            IdGenerator generator,
            EntityTypeMappingRegistry.Mapping generatedKeyMapping) {
        EntityMetadata<T> safeMetadata = Objects.requireNonNull(metadata, "entity metadata must not be null");
        IdGenerator safeGenerator = Objects.requireNonNull(generator, "id generator must not be null");
        List<PrimaryKey> primaryKeys = safeMetadata.fields().stream()
                .filter(EntityFieldMetadata::primaryKey)
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
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        List<Assignment> assignments = new ArrayList<>();
        for (PrimaryKey primaryKey : primaryKeys) {
            Assignment assignment = prepare(safeEntity, primaryKey);
            if (assignment != null) {
                assignments.add(assignment);
            }
        }
        assignments.forEach(assignment -> assignment.access().write(safeEntity, assignment.value()));
    }

    private Assignment prepare(T entity, PrimaryKey primaryKey) {
        EntityFieldMetadata id = primaryKey.field();
        ValueAccess access = primaryKey.access();
        Object current = access.read(entity);
        if (id.generation().generated()) {
            requireNull(current, "database-generated primary key must be null before insert");
            return null;
        }
        return switch (id.idType()) {
            case AUTO -> throw new MappingException("AUTO primary key is missing database generation metadata");
            case NONE, INPUT -> {
                requirePresent(current, "INPUT primary key must be assigned before insert");
                yield null;
            }
            case ASSIGN_UUID -> {
                yield current == null ? new Assignment(access, uuidValue(access.valueType())) : null;
            }
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

    /**
     * 读取数据库生成主键写回前的原值。批量执行器可能先拿到生成键，随后整批事务又回滚，
     * Repository 需要保存这个值，才能把实体恢复到与数据库一致的状态。
     */
    Object currentGeneratedKey(T entity) {
        if (!databaseGenerated()) {
            throw new IllegalStateException("entity does not use a database-generated primary key");
        }
        return generatedKey.access().read(Objects.requireNonNull(entity, "repository entity must not be null"));
    }

    /** 回滚、UNKNOWN 或取消时撤销尚未提交的生成键，避免实体留下并不存在于数据库中的主键。 */
    void restoreGeneratedKey(T entity, Object originalValue) {
        if (!databaseGenerated()) {
            throw new IllegalStateException("entity does not use a database-generated primary key");
        }
        generatedKey.access().write(
                Objects.requireNonNull(entity, "repository entity must not be null"), originalValue);
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
        access.write(safeEntity, generatedKeyValue(value, access.valueType()));
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
        // MySQL 等驱动可能使用 GENERATED_KEY 一类标签；单列结果没有歧义，可以安全采用第一列。
        if (matches == 0 && row.columnCount() == 1) {
            return row.value(0);
        }
        throw new MappingException("generated primary key column is missing or ambiguous: " + id.columnName());
    }

    private static ValueAccess resolveAccess(Class<?> type, EntityFieldMetadata id) {
        Method reader = null;
        Method writer = null;
        try {
            for (PropertyDescriptor property : Introspector.getBeanInfo(type).getPropertyDescriptors()) {
                if (property.getName().equals(id.name())) {
                    reader = accessible(property.getReadMethod(), property.getName());
                    writer = accessible(property.getWriteMethod(), property.getName());
                    break;
                }
            }
        } catch (IntrospectionException error) {
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
        return new ValueAccess(reader, writer, field, reader != null ? reader.getReturnType() : field.getType());
    }

    private static Field findField(Class<?> type, String name) {
        for (Class<?> current = type; current != null && current != Object.class; current = current.getSuperclass()) {
            try {
                Field field = current.getDeclaredField(name);
                if (!EntityMetadataHierarchy.isPersistentField(field)) {
                    // 非持久子字段不能遮住真正的继承主键；回填和回滚恢复都必须使用持久成员。
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

    private static void requireNull(Object value, String message) {
        if (value != null) throw new MappingException(message);
    }

    private static void requirePresent(Object value, String message) {
        if (value == null) throw new MappingException(message);
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
