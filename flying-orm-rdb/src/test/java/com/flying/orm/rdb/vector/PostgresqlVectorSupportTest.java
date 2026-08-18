package com.flying.orm.rdb.vector;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionErrorCode;
import com.flying.orm.core.condition.StructuredConditionException;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.schema.FormSchemaSqlRenderer;
import org.junit.jupiter.api.Test;

import java.util.AbstractCollection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** PostgreSQL Vector 的 DDL、值转换、结构化条件和最近邻查询共用这一组小型契约测试。 */
class PostgresqlVectorSupportTest {

    @Test
    void rendersVectorColumnAndBindsFiniteFloatArray() {
        DynamicForm form = vectorForm();
        FormSchemaSqlRenderer schema = FormSchemaSqlRenderer.create(RdbDialect.postgresql().schema());
        FormDataSqlRenderer data = FormDataSqlRenderer.create(defaultRenderer(), RdbDialect.postgresql());
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", 1L);
        values.put("embedding", List.of(0.1, 0.2, 0.3));

        assertEquals("create table \"documents\" (\"id\" BIGINT primary key, \"embedding\" VECTOR(3))",
                     schema.createTable(form).getFirst().sql());

        SqlRequest insert = data.insert(form, values);
        assertEquals("insert into \"documents\" (\"id\", \"embedding\") values (?, ?)", insert.sql());
        assertArrayEquals(new float[]{0.1F, 0.2F, 0.3F}, (float[]) insert.parameters().get(1), 0.00001F);
    }

    @Test
    void compilesFrontendVectorDistanceWithoutPuttingValuesIntoSql() {
        StructuredConditionInput input = StructuredConditionInput.term(
                "embedding",
                "vector-cosine-lt",
                Map.of("vector", List.of(0.1, 0.2, 0.3), "distance", 0.25));

        ConditionGroup where = VectorStructuredConditions.postgresql().compile(vectorForm(), input);
        SqlFragment fragment = SqlRenderer.builder()
                                          .addTermPackage(VectorTermHandlers.postgresql())
                                          .build()
                                          .renderWhere(where);

        assertEquals("(embedding <=> cast(? as vector)) < ?", fragment.sql());
        assertArrayEquals(new float[]{0.1F, 0.2F, 0.3F}, (float[]) fragment.parameters().getFirst(), 0.00001F);
        assertEquals(0.25D, fragment.parameters().get(1));
    }

    /** Vector 维度由字段模型与 codec 管理，不能被通用前端集合上限误杀。 */
    @Test
    void acceptsVectorDimensionsAboveGenericCollectionLimit() {
        int dimensions = 1_536;
        ConditionGroup where = VectorStructuredConditions.postgresql().compile(
                vectorForm(dimensions),
                StructuredConditionInput.term(
                        "embedding",
                        "vector-cosine-lt",
                        Map.of("vector", Collections.nCopies(dimensions, 0.1), "distance", 0.25)),
                StructuredConditionPolicy.defaults().withMaxCollectionSize(2));
        SqlFragment fragment = SqlRenderer.builder()
                                          .addTermPackage(VectorTermHandlers.postgresql())
                                          .build()
                                          .renderWhere(where);

        assertEquals(dimensions, ((float[]) fragment.parameters().getFirst()).length);
    }

    @Test
    void rendersScopedNearestNeighbourQueryAndRejectsBadVectors() {
        PostgresqlVectorQueryRenderer renderer = PostgresqlVectorQueryRenderer.create(defaultRenderer());
        ConditionGroup scope = ConditionGroup.and().where("tenant_id", "=", "t-1").build();

        DynamicForm form = DynamicForm.builder("documents", "documents")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("tenant_id", "VARCHAR"))
                                      .addField(DynamicField.of("embedding", "VECTOR").withLength(3))
                                      .build();
        SqlRequest request = renderer.nearest(form,
                                              List.of("id", "tenant_id"),
                                              "embedding",
                                              List.of(0.1, 0.2, 0.3),
                                              VectorMetric.COSINE,
                                              scope,
                                              10);

        assertEquals("select \"id\", \"tenant_id\", \"embedding\" <=> cast(? as vector) as \"_distance\" "
                             + "from \"documents\" where \"tenant_id\" = ? order by \"_distance\" asc limit ?",
                     request.sql());
        assertArrayEquals(new float[]{0.1F, 0.2F, 0.3F}, (float[]) request.parameters().getFirst(), 0.00001F);
        assertEquals(List.of("t-1", 10), request.parameters().subList(1, 3));

        assertThrows(IllegalArgumentException.class,
                     () -> VectorValueCodec.write(List.of(0.1, 0.2), 3));
        assertThrows(IllegalArgumentException.class,
                     () -> VectorValueCodec.write(List.of(0.1, Double.NaN, 0.3), 3));
    }

    /** 最近邻查询的普通条件必须复用 DynamicForm 字段 codec，不能把 UUID 原样绑定到 VARCHAR。 */
    @Test
    void convertsNearestQueryConditionsWithDynamicFieldMetadata() {
        PostgresqlVectorQueryRenderer renderer = PostgresqlVectorQueryRenderer.create(defaultRenderer());
        UUID tenantId = UUID.fromString("d6e696a0-4892-4dd5-9d4d-da2490c8aaad");
        DynamicForm form = DynamicForm.builder("documents", "documents")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("tenant_id", "VARCHAR"))
                                      .addField(DynamicField.of("embedding", "VECTOR").withLength(3))
                                      .build();

        SqlRequest request = renderer.nearest(
                form,
                List.of("id", "tenant_id"),
                "embedding",
                List.of(0.1, 0.2, 0.3),
                VectorMetric.COSINE,
                ConditionGroup.and().where("TENANT_ID", "=", tenantId).build(),
                10);

        assertEquals("select \"id\", \"tenant_id\", \"embedding\" <=> cast(? as vector) as \"_distance\" "
                             + "from \"documents\" where \"tenant_id\" = ? order by \"_distance\" asc limit ?",
                     request.sql());
        assertEquals(tenantId.toString(), request.parameters().get(1));
    }

    /** 超大 Collection 必须按向量维度上限有界读取，不能先调用 toArray 复制全部输入。 */
    @Test
    void rejectsOversizedVectorCollectionBeforeBulkCopyingIt() {
        AtomicInteger reads = new AtomicInteger();
        AbstractCollection<Number> oversized = new AbstractCollection<>() {
            @Override
            public Iterator<Number> iterator() {
                return new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return true;
                    }

                    @Override
                    public Number next() {
                        reads.incrementAndGet();
                        return 1F;
                    }
                };
            }

            @Override
            public int size() {
                return Integer.MAX_VALUE;
            }

            @Override
            public Object[] toArray() {
                throw new AssertionError("vector collection must not be bulk-copied before validation");
            }
        };

        assertThrows(IllegalArgumentException.class, () -> VectorValueCodec.write(oversized, null));
        assertTrue(reads.get() <= VectorValueCodec.MAX_DIMENSIONS + 1);
    }

    private static DynamicForm vectorForm() {
        return vectorForm(3);
    }

    private static DynamicForm vectorForm(int dimensions) {
        return DynamicForm.builder("documents", "documents")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("embedding", "VECTOR").withLength(dimensions))
                          .build();
    }

    private static SqlRenderer defaultRenderer() {
        return SqlRenderer.builder().addDefaultTerms().build();
    }
}
