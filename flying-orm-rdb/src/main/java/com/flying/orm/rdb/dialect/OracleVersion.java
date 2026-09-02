package com.flying.orm.rdb.dialect;

/**
 * 当前代码层明确覆盖的 Oracle 版本线。12c 是 OFFSET/FETCH 和标识列的最低边界；19c 是默认保守基线；
 * 21c 才启用原生 JSON 列；23ai 才把逻辑 BOOLEAN 映射成 SQL BOOLEAN。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public enum OracleVersion {
    V12C("12c", false, false),
    V19C("19c", false, false),
    V21C("21c", true, false),
    V23AI("23ai", true, true);

    private final String label;
    private final boolean nativeJson;
    private final boolean nativeBoolean;

    OracleVersion(String label, boolean nativeJson, boolean nativeBoolean) {
        this.label = label;
        this.nativeJson = nativeJson;
        this.nativeBoolean = nativeBoolean;
    }

    public String label() {
        return label;
    }

    boolean nativeJson() {
        return nativeJson;
    }

    boolean nativeBoolean() {
        return nativeBoolean;
    }
}
