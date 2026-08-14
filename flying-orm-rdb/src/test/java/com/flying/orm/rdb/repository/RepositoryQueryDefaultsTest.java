package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.OrderBy;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.internal.mapping.EntityMetadataResolver;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** 验证实体默认投影和排序只使用结构化字段，不接受 SQL 片段。 */
class RepositoryQueryDefaultsTest {

    @Test
    void appliesSelectFalseAndOrderedFieldsToOneQuerySpec() {
        var metadata = EntityMetadataResolver.createUncached(AuditRecord.class);
        QuerySpec spec = RepositoryQueryDefaults.apply(
                QuerySpec.of(metadata.toDynamicForm(), ConditionGroup.and().build()), metadata);

        assertEquals(List.of("id", "created_at"), spec.projections());
        assertEquals(List.of("created_at"), spec.sorts().stream().map(sort -> sort.field()).toList());
        assertEquals("desc", spec.sorts().getFirst().sqlKeyword());
    }

    @TableName("audit_record")
    private static final class AuditRecord {
        @TableId
        private Long id;

        @TableField(select = false)
        private String secret;

        @TableField("created_at")
        @OrderBy(asc = false, sort = 10)
        private Instant createdAt;
    }
}
