package com.flying.orm.rdb.json;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 验证 JSON codec 能接住真实驱动返回的常见包装值，不把驱动类型泄漏给上层表单 API。 */
public class JsonValueCodecTest {

    @Test
    void readsDriverJsonWrapperThroughItsPublicTextAccessor() {
        DriverJson value = new InternalDriverJson("{\"name\":\"Alice\",\"roles\":[\"admin\"]}");

        Object decoded = JsonValueCodec.read(value);

        assertEquals(Map.of("name", "Alice", "roles", List.of("admin")), decoded);
    }

    /** Jackson 包装 getter 的 JVM 致命错误后，codec 仍必须把原对象传播出去。 */
    @Test
    void writePropagatesFatalHiddenByJackson() {
        OutOfMemoryError fatal = new OutOfMemoryError("fatal-json-write");

        OutOfMemoryError actual = assertThrows(
                OutOfMemoryError.class, () -> JsonValueCodec.write(new FatalJsonBean(fatal)));

        assertSame(fatal, actual);
    }

    /** JSON 条件字面量和字段写入必须使用相同的 fatal 传播规则。 */
    @Test
    void writeLiteralPropagatesFatalHiddenByJackson() {
        OutOfMemoryError fatal = new OutOfMemoryError("fatal-json-literal");

        OutOfMemoryError actual = assertThrows(
                OutOfMemoryError.class, () -> JsonValueCodec.writeLiteral(new FatalJsonBean(fatal)));

        assertSame(fatal, actual);
    }

    /** 实体 JSON 反序列化的 setter 包装也不能把 JVM 致命错误降级成普通转换失败。 */
    @Test
    void typedReadPropagatesFatalHiddenByJackson() {
        OutOfMemoryError fatal = new OutOfMemoryError("fatal-json-read");
        FatalReadBean.fatal.set(fatal);
        try {
            OutOfMemoryError actual = assertThrows(
                    OutOfMemoryError.class,
                    () -> JsonValueCodec.read("{\"value\":\"x\"}", FatalReadBean.class));

            assertSame(fatal, actual);
        } finally {
            FatalReadBean.fatal.remove();
        }
    }

    /** 形状和 PostgreSQL R2DBC 的 Json 包装器一致，测试不需要把具体驱动带进主模块。 */
    public static class DriverJson {

        private final String json;

        public DriverJson(String json) {
            this.json = json;
        }

        public String asString() {
            return json;
        }
    }

    /** PostgreSQL 驱动也是公开 Json 父类、包私有具体实现，访问器必须从公开层级取得。 */
    private static final class InternalDriverJson extends DriverJson {

        private InternalDriverJson(String json) {
            super(json);
        }

        @Override
        public String asString() {
            return super.asString();
        }
    }

    /** 用同一个预构造 fatal 验证异常身份，避免只验证异常类型而漏掉二次包装。 */
    public static final class FatalJsonBean {

        private final OutOfMemoryError fatal;

        private FatalJsonBean(OutOfMemoryError fatal) {
            this.fatal = fatal;
        }

        public String getValue() {
            throw new IllegalArgumentException("wrapped-json-failure", fatal);
        }
    }

    public static final class FatalReadBean {

        private static final ThreadLocal<OutOfMemoryError> fatal = new ThreadLocal<>();

        public void setValue(String ignored) {
            throw new IllegalArgumentException("wrapped-json-read-failure", fatal.get());
        }
    }
}
