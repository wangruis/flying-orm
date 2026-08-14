package com.flying.orm.rdb.schema;

import com.flying.orm.core.sql.render.SqlRequest;

import java.util.List;
import java.util.Objects;

/** DDL 前后的会话保护 SQL，必须由同连接执行器使用，不能拆成普通的独立 SQL 调用。 */
record SchemaDdlSessionGuard(List<SqlRequest> setup, List<SqlRequest> cleanup) {
    SchemaDdlSessionGuard {
        setup = List.copyOf(Objects.requireNonNull(setup, "DDL guard setup must not be null"));
        cleanup = List.copyOf(Objects.requireNonNull(cleanup, "DDL guard cleanup must not be null"));
    }

    public List<String> setupSqlTexts() {
        return setup.stream().map(SqlRequest::sql).toList();
    }

    public List<String> cleanupSqlTexts() {
        return cleanup.stream().map(SqlRequest::sql).toList();
    }
}
