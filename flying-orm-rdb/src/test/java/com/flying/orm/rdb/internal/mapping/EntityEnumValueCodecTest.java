package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.rdb.mapping.MappingException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    /** 重复声明只报告枚举位置，不能把任意长度的数据库值复制到启动错误。 */
    @Test
    void duplicateDatabaseValueIsNotExposed() {
        String secret = duplicateSecret();

        MappingException failure = assertThrows(
                MappingException.class,
                () -> EntityEnumValueCodec.create(DuplicateTextStatus.class, "code"));

        assertFalse(failure.getMessage().contains(secret));
        assertTrue(failure.getMessage().contains("FIRST"));
        assertTrue(failure.getMessage().contains("SECOND"));
    }

    /** 重复值诊断不能调用不受控业务对象的 toString。 */
    @Test
    void duplicateDatabaseValueDoesNotInvokeToString() {
        MappingException failure = assertThrows(
                MappingException.class,
                () -> EntityEnumValueCodec.create(DuplicateObjectStatus.class, "code"));

        assertTrue(failure.getMessage().contains("FIRST"));
        assertTrue(failure.getMessage().contains("SECOND"));
    }

    private enum Status {
        ACTIVE("A");

        private final String code;

        Status(String code) {
            this.code = code;
        }
    }

    private enum DuplicateTextStatus {
        FIRST(duplicateSecret()),
        SECOND(duplicateSecret());

        private final String code;

        DuplicateTextStatus(String code) {
            this.code = code;
        }
    }

    private static String duplicateSecret() {
        return "credential-fragment-" + "x".repeat(4096);
    }

    private enum DuplicateObjectStatus {
        FIRST(new DuplicateCode()),
        SECOND(new DuplicateCode());

        private final DuplicateCode code;

        DuplicateObjectStatus(DuplicateCode code) {
            this.code = code;
        }
    }

    private static final class DuplicateCode {

        @Override
        public boolean equals(Object candidate) {
            return candidate instanceof DuplicateCode;
        }

        @Override
        public int hashCode() {
            return 1;
        }

        @Override
        public String toString() {
            throw new AssertionError("duplicate enum value must not be stringified");
        }
    }
}
