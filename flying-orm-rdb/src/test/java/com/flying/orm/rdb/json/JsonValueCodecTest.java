package com.flying.orm.rdb.json;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** 验证 JSON codec 能接住真实驱动返回的常见包装值，不把驱动类型泄漏给上层表单 API。 */
public class JsonValueCodecTest {

    @Test
    void readsDriverJsonWrapperThroughItsPublicTextAccessor() {
        DriverJson value = new InternalDriverJson("{\"name\":\"Alice\",\"roles\":[\"admin\"]}");

        Object decoded = JsonValueCodec.read(value);

        assertEquals(Map.of("name", "Alice", "roles", List.of("admin")), decoded);
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
}
