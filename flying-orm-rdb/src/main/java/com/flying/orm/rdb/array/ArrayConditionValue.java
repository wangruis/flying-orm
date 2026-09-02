package com.flying.orm.rdb.array;

import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.codec.ArrayValueCodec;
import com.flying.orm.rdb.type.DatabaseTypes;

import java.lang.reflect.Array;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * 数组条件的内部值。保存只读元素列表和受信任字段类型，真正渲染时再创建驱动数组参数。
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public record ArrayConditionValue(List<Object> values, DatabaseType databaseType) {

    public ArrayConditionValue {
        List<Object> source = Objects.requireNonNull(values, "array condition values must not be null");
        if (source.isEmpty()) {
            throw new IllegalArgumentException("array condition values must not be empty");
        }
        databaseType = Objects.requireNonNull(databaseType, "array condition data type must not be null");
        if (!ArrayValueCodec.isArrayDataType(databaseType)) {
            throw new IllegalArgumentException("array condition field must use an SQL array type");
        }
        // 条件 SQL 需要显式 cast。这里先收紧成受支持类型，不能等渲染时把调用方给的类型名原样拼进去。
        DatabaseTypes.postgresqlArrayCast(databaseType);
        // 在发布边界一次性转成字段元素类型；之后只保留这一份只读规范表示。
        List<Object> canonical = new ArrayList<>(source.size());
        for (Object value : source) {
            canonical.add(value == null ? null : ArrayValueCodec.writeElement(value, databaseType));
        }
        values = Collections.unmodifiableList(canonical);
    }

    public ArrayConditionValue(List<Object> values, String dataType) {
        this(values, DatabaseType.of(dataType));
    }

    public static ArrayConditionValue of(Object value, String dataType) {
        return of(value, DatabaseType.of(dataType));
    }

    public static ArrayConditionValue of(Object value, DatabaseType databaseType) {
        return new ArrayConditionValue(inputValues(value), databaseType);
    }

    /**
     * 保留 3.0.x 的字符串类型访问器，避免已编译调用方在 3.1.0 升级后发生链接错误。
     * 新代码可使用 record 生成的 {@link #databaseType()} 获取结构化类型。
     */
    public String dataType() {
        return databaseType.declaration();
    }

    public Object parameter() {
        Class<?> componentType = ArrayValueCodec.parameterType(databaseType).getComponentType();
        Object parameter = Array.newInstance(componentType, values.size());
        for (int index = 0; index < values.size(); index++) {
            Array.set(parameter, index, values.get(index));
        }
        return parameter;
    }

    /**
     * 返回 PostgreSQL 能直接放进 cast 的固定类型名。
     *
     * <p>映射只看动态字段已经声明的逻辑类型，不接收任意 SQL 片段。这样既能避免 String[] 被驱动推断成
     * text[] 后和 varchar[] 字段运算失败，也不会为了修类型问题重新打开 SQL 注入入口。</p>
     */
    String postgresqlCastType() {
        return DatabaseTypes.postgresqlArrayCast(databaseType);
    }

    @SuppressWarnings("unchecked")
    private static List<Object> inputValues(Object value) {
        Object source = Objects.requireNonNull(value, "array condition values must not be null");
        if (source instanceof List<?> list) {
            return (List<Object>) list;
        }
        if (source instanceof Collection<?> collection) {
            return new AbstractList<>() {
                @Override
                public Object get(int index) {
                    if (index < 0 || index >= collection.size()) {
                        throw new IndexOutOfBoundsException(index);
                    }
                    Iterator<?> iterator = collection.iterator();
                    for (int current = 0; current < index; current++) {
                        iterator.next();
                    }
                    return iterator.next();
                }

                @Override
                @SuppressWarnings("unchecked")
                public Iterator<Object> iterator() {
                    return (Iterator<Object>) collection.iterator();
                }

                @Override
                public int size() {
                    return collection.size();
                }
            };
        }
        Class<?> type = source.getClass();
        if (!type.isArray()) {
            throw new IllegalArgumentException("array value must be a Java array or Collection");
        }
        return new AbstractList<>() {
            @Override
            public Object get(int index) {
                return Array.get(source, index);
            }

            @Override
            public int size() {
                return Array.getLength(source);
            }
        };
    }
}
