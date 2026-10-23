package com.flying.orm.rdb.schema;

import com.flying.orm.core.sql.render.SqlRequest;

import java.util.Objects;
import java.util.List;

/**
 * 一项结构操作及其按执行顺序保存的 SQL。
 *
 * <p>顺序由计划列表表达，不另存序号。每个步骤必须有 SQL；未知事实仅属于结构比较结果。</p>
 *
 * @author wangr
 * @version v3.2
 */
public record SchemaPlanStep(SchemaOperation operation, List<SqlRequest> requests) {

    public SchemaPlanStep {
        operation = Objects.requireNonNull(operation, "schema plan operation must not be null");
        requests = List.copyOf(requests);
        if (requests.isEmpty()) {
            throw new IllegalArgumentException("schema plan step must contain SQL");
        }
        if (operation.kind() == SchemaOperation.Kind.VERIFY_MANUALLY) {
            throw new IllegalArgumentException("unknown schema facts cannot have executable SQL");
        }
        for (SqlRequest request : requests) {
            if (!request.parameters().isEmpty()) {
                throw new IllegalArgumentException("schema DDL must not contain bound parameters");
            }
        }
    }

}
