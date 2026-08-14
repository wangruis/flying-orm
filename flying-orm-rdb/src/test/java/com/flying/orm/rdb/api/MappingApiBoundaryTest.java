package com.flying.orm.rdb.api;

import com.flying.orm.rdb.mapping.RowMapper;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * 只检查业务真正需要依赖的映射入口，避免把内部反射计划误当成长期公共 API。
 */
class MappingApiBoundaryTest {

    @Test
    void exposesRowMapperButKeepsCompiledPlanInternal() throws ClassNotFoundException {
        RowMapper<UserRow> mapper = RowMapper.of(UserRow.class);

        assertEquals(new UserRow(7L, "Alice"), mapper.map(Map.of("id", 7L, "name", "Alice")));
        Class<?> planType = Class.forName("com.flying.orm.rdb.mapping.MappingPlan");
        assertFalse(Modifier.isPublic(planType.getModifiers()));
    }

    private record UserRow(Long id, String name) {
    }
}
