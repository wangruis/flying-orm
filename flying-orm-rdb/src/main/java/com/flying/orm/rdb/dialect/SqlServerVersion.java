package com.flying.orm.rdb.dialect;

/**
 * 当前代码层明确覆盖的 SQL Server 版本线。2012 是 OFFSET/FETCH 和序列的最低边界；
 * 2016 起才声明 JSON 函数能力。JSON 列仍使用 NVARCHAR(max)，不启用预览中的原生 JSON 类型。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public enum SqlServerVersion {
    V2012("2012", false),
    V2016("2016", true),
    V2019("2019", true),
    V2022("2022", true);

    private final String label;
    private final boolean jsonFunctions;

    SqlServerVersion(String label, boolean jsonFunctions) {
        this.label = label;
        this.jsonFunctions = jsonFunctions;
    }

    public String label() {
        return label;
    }

    boolean jsonFunctions() {
        return jsonFunctions;
    }
}
