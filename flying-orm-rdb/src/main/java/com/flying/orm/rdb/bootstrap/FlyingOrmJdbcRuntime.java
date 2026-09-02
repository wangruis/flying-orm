package com.flying.orm.rdb.bootstrap;

import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.SyncFormMetadataReader;
import com.flying.orm.rdb.operator.SyncDatabaseOperator;
import com.flying.orm.rdb.schema.JdbcSchemaClient;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.util.Objects;

/** 已装配完成的 JDBC 对象组；所有组件共享同一 DataSource 和事务参与者。 */
record FlyingOrmJdbcRuntime(SyncSqlExecutor executor,
                            SyncFormClient forms,
                            JdbcSchemaClient schema,
                            JdbcFormMetadataReader jdbcMetadata,
                            SyncFormMetadataReader metadata,
                            SyncDatabaseOperator operator) {

    FlyingOrmJdbcRuntime {
        executor = Objects.requireNonNull(executor, "sync sql executor must not be null");
        forms = Objects.requireNonNull(forms, "sync form client must not be null");
        schema = Objects.requireNonNull(schema, "jdbc schema client must not be null");
        jdbcMetadata = Objects.requireNonNull(jdbcMetadata, "jdbc metadata reader must not be null");
        metadata = Objects.requireNonNull(metadata, "sync metadata reader must not be null");
        operator = Objects.requireNonNull(operator, "sync database operator must not be null");
    }
}
