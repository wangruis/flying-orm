package com.flying.orm.rdb.array;

import com.flying.orm.rdb.codec.ArrayValueCodec;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * 数组条件的内部值。保存只读元素列表和受信任字段类型，真正渲染时再创建驱动数组参数。
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public record ArrayConditionValue(List<Object> values, String dataType) {

    public ArrayConditionValue {
        values = List.copyOf(Objects.requireNonNull(values, "array condition values must not be null"));
        if (values.isEmpty()) {
            throw new IllegalArgumentException("array condition values must not be empty");
        }
        dataType = Objects.requireNonNull(dataType, "array condition data type must not be null").trim();
        if (!ArrayValueCodec.isArrayDataType(dataType)) {
            throw new IllegalArgumentException("array condition field must use an SQL array type");
        }
        // 条件 SQL 需要显式 cast。这里先收紧成受支持类型，不能等渲染时把调用方给的类型名原样拼进去。
        postgresqlCastType(dataType);
        // 构造时就按字段元素类型完成转换，不能把可变 CharSequence 等调用方对象保留到延迟 SQL 渲染阶段。
        values = List.copyOf(ArrayValueCodec.read(ArrayValueCodec.write(values, dataType)));
    }

    public static ArrayConditionValue of(Object value, String dataType) {
        return new ArrayConditionValue(ArrayValueCodec.read(value), dataType);
    }

    public Object parameter() {
        return ArrayValueCodec.write(values, dataType);
    }

    /**
     * 返回 PostgreSQL 能直接放进 cast 的固定类型名。
     *
     * <p>映射只看动态字段已经声明的逻辑类型，不接收任意 SQL 片段。这样既能避免 String[] 被驱动推断成
     * text[] 后和 varchar[] 字段运算失败，也不会为了修类型问题重新打开 SQL 注入入口。</p>
     */
    String postgresqlCastType() {
        return postgresqlCastType(dataType);
    }

    private static String postgresqlCastType(String dataType) {
        String elementType = dataType.substring(0, dataType.length() - 2).trim().toUpperCase(Locale.ROOT);
        int arguments = elementType.indexOf('(');
        if (arguments >= 0) {
            elementType = elementType.substring(0, arguments).trim();
        }
        return switch (elementType) {
            case "BIGINT", "INT8", "BIGSERIAL" -> "bigint[]";
            case "SMALLINT", "INT2" -> "smallint[]";
            case "INTEGER", "INT", "INT4", "SERIAL" -> "integer[]";
            case "DECIMAL", "NUMERIC" -> "numeric[]";
            case "DOUBLE", "DOUBLE PRECISION", "FLOAT", "FLOAT8" -> "double precision[]";
            case "REAL", "FLOAT4" -> "real[]";
            case "BOOLEAN", "BOOL" -> "boolean[]";
            case "DATE" -> "date[]";
            case "TIME", "TIME WITHOUT TIME ZONE" -> "time[]";
            case "TIME WITH TIME ZONE", "TIMETZ" -> "time with time zone[]";
            case "TIMESTAMP", "DATETIME", "TIMESTAMP WITHOUT TIME ZONE" -> "timestamp[]";
            case "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ" -> "timestamp with time zone[]";
            case "UUID" -> "uuid[]";
            case "VARCHAR", "CHARACTER VARYING" -> "varchar[]";
            case "CHAR", "CHARACTER", "BPCHAR" -> "character[]";
            case "TEXT" -> "text[]";
            default -> throw new IllegalArgumentException("unsupported PostgreSQL array condition type");
        };
    }
}
