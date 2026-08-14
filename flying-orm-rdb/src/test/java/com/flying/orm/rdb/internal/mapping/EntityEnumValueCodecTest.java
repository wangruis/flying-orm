package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.rdb.mapping.MappingException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证实体枚举值映射的错误边界不会泄露数据库业务值。
 *
 * @author wangr
 * @date 2026-08-08
 * @version v1.0
 */
class EntityEnumValueCodecTest {

    /** 未注册的数据库枚举值只报告稳定类型，不回显原始值。 */
    @Test
    void unknownDatabaseValueIsNotExposed() {
        String secret = "password=must-not-leak";
        EntityEnumValueCodec codec = EntityEnumValueCodec.create(Status.class, "code");

        MappingException failure = assertThrows(
                MappingException.class,
                () -> codec.read(secret, ValueCodecRegistry.standard()));

        assertFalse(failure.getMessage().contains(secret));
    }

    private enum Status {
        ACTIVE("A");

        private final String code;

        Status(String code) {
            this.code = code;
        }
    }
}
