package com.flying.orm.rdb.vector;

/**
 * PostgreSQL pgvector 的距离算法。枚举把 SQL 操作符固定在框架代码里，调用方只能选语义，不能传任意 SQL。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public enum VectorMetric {
    /** 欧氏距离，越小越近。 */
    L2("<->"),
    /** 余弦距离，越小越近。 */
    COSINE("<=>"),
    /** 内积查询使用 pgvector 的负内积操作符，仍然按升序走向量索引。 */
    INNER_PRODUCT("<#>");

    private final String operator;

    VectorMetric(String operator) {
        this.operator = operator;
    }

    String operator() {
        return operator;
    }
}
