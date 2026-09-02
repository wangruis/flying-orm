package com.flying.orm.rdb.bootstrap;

import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.operator.DatabaseOperator;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.schema.ReactiveSchemaClient;

import java.util.Objects;

/** 已装配完成的 R2DBC 对象组；缺少 R2DBC 时整个对象都不存在，也不会生成响应式客户端。 */
record FlyingOrmReactiveRuntime(ReactiveSqlExecutor executor,
                                ReactiveFormClient forms,
                                ReactiveSchemaClient schema,
                                ReactiveFormMetadataReader metadata,
                                DatabaseOperator operator) {

    FlyingOrmReactiveRuntime {
        executor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        forms = Objects.requireNonNull(forms, "reactive form client must not be null");
        schema = Objects.requireNonNull(schema, "reactive schema client must not be null");
        metadata = Objects.requireNonNull(metadata, "reactive metadata reader must not be null");
        operator = Objects.requireNonNull(operator, "reactive database operator must not be null");
    }
}
