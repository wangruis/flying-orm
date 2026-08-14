package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.lambda.EntityProperty;
import com.flying.orm.rdb.internal.ReflectionFailureSupport;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.MappingException;

import java.beans.Introspector;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.RecordComponent;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 把可序列化实体方法引用解析为受实体元数据约束的数据库列名。
 *
 * <p>解析结果按 JVM 生成的 Lambda 类使用 {@link ClassValue} 缓存。这样热路径不重复执行反射，
 * 同时应用类卸载时缓存不会永久持有业务 ClassLoader。解析只接受直接 getter、布尔 getter或
 * record 访问器；包含 {@code lambda$} 的普通 Lambda、跨实体引用和不存在的字段都会在生成 SQL
 * 之前失败，避免错误列名进入执行阶段。</p>
 *
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class EntityPropertyResolver {

    private static final ClassValue<AtomicReference<LambdaMethod>> CACHE = new ClassValue<>() {
        @Override
        protected AtomicReference<LambdaMethod> computeValue(Class<?> ignored) {
            return new AtomicReference<>();
        }
    };

    private EntityPropertyResolver() {
    }

    /**
     * 使用调用方已经缓存的实体元数据解析列名，避免客户端热路径落入另一份全局元数据缓存。
     *
     * @param metadata 当前客户端实例持有的实体元数据
     * @param property 直接实体属性方法引用
     * @param <T> 实体类型
     * @return 经过所属类型和持久化字段校验的物理列名
     */
    public static <T> String column(EntityMetadata<T> metadata, EntityProperty<T, ?> property) {
        EntityMetadata<T> safeMetadata = Objects.requireNonNull(metadata, "entity metadata must not be null");
        Class<T> safeType = safeMetadata.type();
        EntityProperty<T, ?> safeProperty = Objects.requireNonNull(property, "entity property must not be null");
        AtomicReference<LambdaMethod> slot = CACHE.get(safeProperty.getClass());
        LambdaMethod method = slot.get();
        if (method == null) {
            LambdaMethod resolved = inspect(safeProperty);
            slot.compareAndSet(null, resolved);
            method = slot.get();
        }
        if (!method.owner().isAssignableFrom(safeType)) {
            throw new MappingException("entity property belongs to another type: " + method.owner().getName());
        }
        try {
            return safeMetadata.field(method.property()).columnName();
        } catch (IllegalArgumentException error) {
            throw new MappingException("entity property is not persistent: " + safeType.getName()
                                               + "." + method.property(), error);
        }
    }

    private static LambdaMethod inspect(Object property) {
        Class<?> lambdaType = property.getClass();
        try {
            Method replacement = lambdaType.getDeclaredMethod("writeReplace");
            if (!replacement.trySetAccessible()) {
                throw new MappingException("entity property does not allow reflective inspection");
            }
            Object value = replacement.invoke(property);
            if (!(value instanceof SerializedLambda lambda)) {
                throw new MappingException("entity property does not expose SerializedLambda: " + lambdaType.getName());
            }
            return from(lambda, lambdaType.getClassLoader());
        } catch (MappingException error) {
            throw error;
        } catch (ReflectiveOperationException | SecurityException error) {
            ReflectionFailureSupport.rethrowVirtualMachineError(error);
            throw new MappingException("entity property cannot be inspected", error);
        }
    }

    private static LambdaMethod from(SerializedLambda lambda, ClassLoader loader) throws ClassNotFoundException {
        String method = lambda.getImplMethodName();
        if (method.startsWith("lambda$")) {
            throw new MappingException("only direct entity getter or record accessor references are supported");
        }
        Class<?> owner = Class.forName(lambda.getImplClass().replace('/', '.'), false, loader);
        Method accessor;
        try {
            accessor = owner.getDeclaredMethod(method);
        } catch (NoSuchMethodException error) {
            throw new MappingException("entity property accessor does not exist: " + owner.getName() + "." + method,
                                       error);
        }
        int modifiers = accessor.getModifiers();
        if (Modifier.isStatic(modifiers) || accessor.isDefault() || accessor.getParameterCount() != 0
                || void.class.equals(accessor.getReturnType())) {
            throw new MappingException("method is not a direct entity property accessor: " + accessor);
        }
        return new LambdaMethod(owner, propertyName(owner, accessor));
    }

    private static String propertyName(Class<?> owner, Method accessor) {
        String method = accessor.getName();
        if (method.startsWith("get") && method.length() > 3) {
            return Introspector.decapitalize(method.substring(3));
        }
        if (method.startsWith("is") && method.length() > 2) {
            Class<?> returnType = accessor.getReturnType();
            if (!boolean.class.equals(returnType) && !Boolean.class.equals(returnType)) {
                throw new MappingException("is-prefix accessor must return boolean: " + accessor);
            }
            return Introspector.decapitalize(method.substring(2));
        }
        if (owner.isRecord()) {
            for (RecordComponent component : owner.getRecordComponents()) {
                if (component.getName().equals(method) && component.getAccessor().equals(accessor)) {
                    return method;
                }
            }
        }
        throw new MappingException("method is not a JavaBean getter or record accessor: " + accessor);
    }

    private record LambdaMethod(Class<?> owner, String property) {
    }
}
