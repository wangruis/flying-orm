package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.dialect.RdbDialect;

import java.util.Objects;

/**
 * flying-orm 对一个数据库方言普通 DDL 事务行为的保守判断。
 *
 * <p>{@link #TRANSACTIONAL} 表示普通结构语句可以加入外部事务，但具体审核计划仍可能包含必须脱离事务执行的
 * 语句，例如 PostgreSQL 的并发索引。{@link #IMPLICIT_COMMIT} 表示数据库会在 DDL 前后自行提交，ORM 不能把它
 * 伪装成可随业务事务回滚。未知自定义方言默认关闭外部事务 DDL，直到使用方明确声明能力。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public enum SchemaDdlTransactionSupport {
    /** 普通 DDL 可以参与外部事务，计划中的特殊非事务语句仍需单独拒绝。 */
    TRANSACTIONAL,
    /** DDL 会隐式提交或无法可靠回滚，不能放进上层业务事务。 */
    IMPLICIT_COMMIT,
    /** 自定义或无法确认的方言，按安全默认值拒绝外部事务 DDL。 */
    UNKNOWN;

    /** 根据内置 RDB 方言取得已确认能力；自定义方言保持 UNKNOWN。 */
    public static SchemaDdlTransactionSupport from(RdbDialect dialect) {
        String name = Objects.requireNonNull(dialect, "rdb dialect must not be null").name();
        return switch (name) {
            case "postgresql", "sqlserver" -> TRANSACTIONAL;
            case "h2", "mysql", "oracle" -> IMPLICIT_COMMIT;
            default -> UNKNOWN;
        };
    }

    /** 返回普通 DDL 能否安全加入外部事务。 */
    public boolean allowsExternalTransaction() {
        return this == TRANSACTIONAL;
    }
}
