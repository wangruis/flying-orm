package com.flying.orm.rdb.mapping;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.annotation.FieldFill;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.internal.error.ThrowableGraph;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.mapping.EntityFieldNames;
import com.flying.orm.rdb.internal.mapping.EntityMetadataHierarchy;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * 保存一个已经编译好的实体写入动作。
 *
 * <p>映射计划只负责决定某一列应该写到哪个成员；这里集中处理 setter、字段反射和类型转换，
 * 避免每映射一行都重新分析注解或成员类型。实例只在映射计划创建时生成，之后可以并发只读复用。</p>
 *
 * @author wangr
 * @version v4.1.2
 */
@InternalApi
public interface EntityValueWriter {

    void write(Object target, Object value, ValueCodecRegistry valueCodecs);

    static EntityValueWriter forMethod(Method method,
                                       EntityRowValueConverter.Reader reader) {
        return (target, value, codecs) -> writeMethod(method, target, reader.read(value, codecs));
    }

    private static void writeMethod(Method method, Object target, Object value) {
        try {
            method.invoke(target, value);
        } catch (ReflectiveOperationException error) {
            ThrowableGraph.rethrowVirtualMachineError(error);
            throw new MappingException("bean setter cannot be written: " + method.getName(), error);
        }
    }

    static EntityValueWriter forField(Field field,
                                      EntityRowValueConverter.Reader reader) {
        return (target, value, codecs) -> writeField(field, target, reader.read(value, codecs));
    }

    private static void writeField(Field field, Object target, Object value) {
        try {
            field.set(target, value);
        } catch (IllegalAccessException error) {
            throw new MappingException("bean field cannot be written: " + field.getName(), error);
        }
    }

    /** 冷路径编译显式回填成员；热路径接收 Java 值，不再次执行数据库 codec。 */
    static Map<String, BiConsumer<Object, Object>> fillWriters(EntityMetadata<?> metadata) {
        Map<String, BiConsumer<Object, Object>> writers = null;
        PropertyDescriptor[] properties = null;
        try {
            for (Field field : EntityMetadataHierarchy.persistentFields(metadata.type())) {
                TableField annotation = field.getAnnotation(TableField.class);
                EntityFieldMetadata mapped = metadata.findField(field.getName()).orElse(null);
                if (annotation == null || !annotation.fillBack() || mapped == null
                        || mapped.fill() == FieldFill.DEFAULT || !mapped.insertable() && !mapped.updatable()) {
                    continue;
                }
                if (properties == null) {
                    properties = Introspector.getBeanInfo(metadata.type()).getPropertyDescriptors();
                    writers = new LinkedHashMap<>();
                }
                Method setter = null;
                for (PropertyDescriptor property : properties) {
                    if (EntityFieldNames.matches(field.getName(), property.getName())
                            && property.getWriteMethod() != null
                            && EntityMetadataHierarchy.isPersistentAccessor(
                                    property.getWriteMethod(), property.getName())) {
                        setter = property.getWriteMethod();
                        break;
                    }
                }
                if (metadata.type().isRecord() || setter == null && Modifier.isFinal(field.getModifiers())) {
                    throw new MappingException("fillBack requires a writable bean property: " + field);
                }
                AccessibleObject member = setter == null ? field : setter;
                if (!member.trySetAccessible()) {
                    throw new MappingException("fillBack member is not accessible: " + member);
                }
                Method writeMethod = setter;
                writers.put(mapped.name(), setter == null
                        ? (target, value) -> writeField(field, target, value)
                        : (target, value) -> writeMethod(writeMethod, target, value));
            }
        } catch (IntrospectionException error) {
            throw new MappingException("entity fillBack plan cannot be created: " + metadata.type().getName(), error);
        }
        return writers == null ? Map.of() : writers;
    }
}
