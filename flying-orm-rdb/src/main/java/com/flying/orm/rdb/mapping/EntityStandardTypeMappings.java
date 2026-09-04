package com.flying.orm.rdb.mapping;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.type.DatabaseType;
import tools.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/** 内置实体类型映射表，只在注册表初始化时创建一次。 */
final class EntityStandardTypeMappings {

    private EntityStandardTypeMappings() {
    }

    static List<EntityTypeMappingRegistry.Mapping> mappings(
            ValueCodec standardCodec,
            ValueCodec jsonCodec) {
        List<EntityTypeMappingRegistry.Mapping> mappings = new ArrayList<>(22);
        add(mappings, "JSON", Map.class, jsonCodec);
        add(mappings, "JSON", Collection.class, jsonCodec);
        add(mappings, "JSON", JsonNode.class, jsonCodec);
        add(mappings, "VARCHAR", String.class, standardCodec);
        add(mappings, "BIGINT", Long.class, standardCodec);
        add(mappings, "INTEGER", Integer.class, standardCodec);
        add(mappings, "INTEGER", Short.class, standardCodec);
        add(mappings, "INTEGER", Byte.class, standardCodec);
        add(mappings, "BOOLEAN", Boolean.class, standardCodec);
        add(mappings, "DECIMAL", BigDecimal.class, standardCodec);
        add(mappings, "DECIMAL", BigInteger.class, standardCodec);
        add(mappings, "DECIMAL", Double.class, standardCodec);
        add(mappings, "DECIMAL", Float.class, standardCodec);
        add(mappings, "TIMESTAMP", LocalDateTime.class, standardCodec);
        add(mappings, "TIMESTAMPTZ", Instant.class, standardCodec);
        add(mappings, "TIMESTAMPTZ", OffsetDateTime.class, standardCodec);
        add(mappings, "DATE", LocalDate.class, standardCodec);
        add(mappings, "TIME", LocalTime.class, standardCodec);
        add(mappings, "OFFSET_TIME", OffsetTime.class, standardCodec);
        add(mappings, "BINARY", byte[].class, standardCodec);
        add(mappings, "BINARY", Byte[].class, standardCodec);
        return List.copyOf(mappings);
    }

    private static void add(
            List<EntityTypeMappingRegistry.Mapping> mappings,
            String id,
            Class<?> javaType,
            ValueCodec codec) {
        mappings.add(new EntityTypeMappingRegistry.Mapping(
                id, javaType, DatabaseType.of(id), codec));
    }
}
