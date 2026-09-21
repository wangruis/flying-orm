package com.flying.orm.rdb.mapping;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.internal.error.ThrowableGraph;
import com.flying.orm.rdb.internal.mapping.EntityEnumValueCodec;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * 保存一个已经编译好的实体写入动作。
 *
 * <p>映射计划只负责决定某一列应该写到哪个成员；这里集中处理 setter、字段反射和类型转换，
 * 避免每映射一行都重新分析注解或成员类型。实例只在映射计划创建时生成，之后可以并发只读复用。</p>
 */
interface EntityValueWriter {

    void write(Object target, Object value, ValueCodecRegistry valueCodecs);

    /** 只在原始行计划编译时包装已注册字段；后续赋值不再重复调用该字段 codec。 */
    static EntityValueWriter fromRaw(EntityValueWriter writer, EntityTypeMappingRegistry.Mapping mapping) {
        return (target, value, valueCodecs) -> writer.write(
                target, EntityRowValueConverter.readCustom(value, mapping), valueCodecs);
    }

    static EntityValueWriter forMethod(Method method,
                                       Class<?> valueType,
                                       EntityFieldMetadata metadata,
                                       ValueCodec customCodec) {
        EntityEnumValueCodec enumValue = EntityRowValueConverter.enumValueCodec(valueType, metadata);
        return (target, value, valueCodecs) -> {
            try {
                method.invoke(target, EntityRowValueConverter.convert(
                        value, valueType, metadata.databaseType(), metadata.enumStorage(), enumValue,
                        customCodec, valueCodecs));
            } catch (ReflectiveOperationException error) {
                ThrowableGraph.rethrowVirtualMachineError(error);
                throw new MappingException("bean setter cannot be written: " + method.getName(), error);
            }
        };
    }

    static EntityValueWriter forField(Field field,
                                      Class<?> valueType,
                                      EntityFieldMetadata metadata,
                                      ValueCodec customCodec) {
        EntityEnumValueCodec enumValue = EntityRowValueConverter.enumValueCodec(valueType, metadata);
        return (target, value, valueCodecs) -> {
            try {
                field.set(target, EntityRowValueConverter.convert(
                        value, valueType, metadata.databaseType(), metadata.enumStorage(), enumValue,
                        customCodec, valueCodecs));
            } catch (IllegalAccessException error) {
                throw new MappingException("bean field cannot be written: " + field.getName(), error);
            }
        };
    }
}
