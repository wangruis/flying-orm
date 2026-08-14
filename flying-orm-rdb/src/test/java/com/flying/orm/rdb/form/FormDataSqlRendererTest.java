package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorSort;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import com.flying.orm.rdb.dialect.PaginationDialect;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.dialect.UpsertDialect;
import com.flying.orm.rdb.schema.SchemaDialect;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.internal.plan.StructuralPlanCaches;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * 验证动态表单数据操作可以渲染为参数化 CRUD SQL。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
class FormDataSqlRendererTest {

    /** 验证批量乐观更新在冷 Publisher 订阅前固定可变二进制字段，调用方不能改写后续 SQL 参数。 */
    @Test
    void snapshotsArrayValuesForDeferredBatchOptimisticUpdate() {
        byte[] source = new byte[]{1, 2, 3};
        BatchOptimisticUpdate update = new BatchOptimisticUpdate(
                orderedMap("payload", source),
                ConditionGroup.and().where("id", "=", "u1").build(),
                OptimisticLockOptions.increment("version", 1L));

        source[0] = 9;
        ((byte[]) update.values().get("payload"))[1] = 8;

        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) update.values().get("payload"));
    }

    /** 嵌套数组的内部节点同样属于冷批量规格，构造后和访问后都不能回写待执行值。 */
    @Test
    void snapshotsNestedArrayGraphsForDeferredBatchOptimisticUpdate() {
        byte[][] source = new byte[][]{{1, 2, 3}};
        BatchOptimisticUpdate update = new BatchOptimisticUpdate(
                orderedMap("payload", source),
                ConditionGroup.and().where("id", "=", "u1").build(),
                OptimisticLockOptions.increment("version", 1L));

        source[0][0] = 9;
        ((byte[][]) update.values().get("payload"))[0][1] = 8;

        assertArrayEquals(new byte[]{1, 2, 3}, ((byte[][]) update.values().get("payload"))[0]);
    }

    @Test
    void reusesCrudSqlByStructureWithoutRetainingBusinessValues() {
        StructuralPlanCaches plans = StructuralPlanCaches.create(OrmCachePolicy.builder()
                .sqlPlans(new CacheRegionPolicy(true, 1_024, 128, Duration.ofMinutes(1), true))
                .conditionPlans(new CacheRegionPolicy(true, 1_024, 128, Duration.ofMinutes(1), true))
                .build());
        FormDataSqlRenderer renderer = renderer().withPlanCaches(plans);

        SqlRequest first = renderer.update(form(), orderedMap("name", "甲", "age", 18),
                                           ConditionGroup.and().where("id", "=", "u1").build());
        SqlRequest second = renderer.update(form(), orderedMap("name", "乙", "age", 27),
                                            ConditionGroup.and().where("id", "=", "u2").build());

        assertSame(first.sql(), second.sql());
        assertEquals(List.of("甲", 18, "u1"), first.parameters());
        assertEquals(List.of("乙", 27, "u2"), second.parameters());
        assertEquals(1L, plans.sqlSnapshot().loadSuccessCount());
        assertEquals(1L, plans.conditionSnapshot().loadSuccessCount());
    }

    @Test
    void bypassesAllStructuralCachesForCustomValueDependentTerm() {
        StructuralPlanCaches plans = StructuralPlanCaches.create(OrmCachePolicy.builder()
                .sqlPlans(new CacheRegionPolicy(true, 1_024, 128, Duration.ofMinutes(1), true))
                .conditionPlans(new CacheRegionPolicy(true, 1_024, 128, Duration.ofMinutes(1), true))
                .build());
        SqlRenderer conditions = SqlRenderer.builder()
                .addTerm(SqlTermHandler.equalsTo())
                .addTerm(SqlTermHandler.of("custom",
                                           (term, context) -> SqlFragment.of(
                                                   context.identifier(term.field()) + " = ?", term.value())))
                .build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, RdbDialect.h2())
                                                           .withPlanCaches(plans);

        renderer.select(form(), ConditionGroup.and(conditions.terms()).where("id", "custom", "u1").build());
        renderer.select(form(), ConditionGroup.and(conditions.terms()).where("id", "custom", "u2").build());

        assertEquals(0L, plans.sqlSnapshot().estimatedSize());
        assertEquals(0L, plans.conditionSnapshot().estimatedSize());
    }

    /**
     * 验证 insert SQL 按调用方数据顺序生成列和参数。
     */
    @Test
    void rendersInsertSqlFromDynamicFormValues() {
        SqlRequest request = renderer().insert(form(), orderedMap("id", "u1", "name", "王"));

        assertEquals("insert into Users (id, name) values (?, ?)", request.sql());
        assertEquals(List.of("u1", "王"), request.parameters());
    }

    /** ASSIGN_UUID 的 UUID 实体字段使用 VARCHAR 逻辑列时必须绑定稳定文本，而不是驱动原生 UUID。 */
    @Test
    void writesUuidAsTextForVarcharFields() {
        UUID value = UUID.fromString("06fb6f53-ae7d-4e2b-8d38-17a72865e726");

        SqlRequest request = renderer().insert(form(), orderedMap("id", 1L, "name", value));

        assertEquals(value.toString(), request.parameters().get(1));
    }

    /**
     * 验证 insert 的归一化重复字段错误不回显调用方提供的超长原始键。
     */
    @Test
    void rejectsDuplicateNormalizedInsertFieldsWithoutEchoingRawCallerKey() {
        String secretRawFieldName = " ".repeat(512) + "name" + " ".repeat(512);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> renderer().insert(form(), orderedMap("name", "first", secretRawFieldName, "second")));

        assertEquals("duplicate normalized dynamic write field", error.getMessage());
        assertFalse(error.getMessage().contains(secretRawFieldName));
    }

    @Test
    void rejectsUpdateDeltaOutsideUpdateSetClause() {
        assertThrows(IllegalArgumentException.class,
                     () -> renderer().insert(form(), orderedMap("id", "u1", "name", UpdateDelta.increment(1))));
    }

    @Test
    void rejectsUpdateDeltaInFirstBatchRowWithCoordinates() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> renderer().insertPlan(form(),
                                            orderedMap("id", "u1", "name", "A", "age",
                                                       UpdateDelta.increment(1))));

        assertEquals("batch write row [0] field [age] does not allow update delta", error.getMessage());
    }

    @Test
    void acceptsOnlyExactNumericTypesForArithmeticUpdates() {
        for (String numeric : List.of("INTEGER", "DECIMAL(18, 2)", "DOUBLE PRECISION", "BIGINT UNSIGNED")) {
            DynamicForm numericForm = DynamicForm.builder("metric", "Metrics")
                                                 .addField(DynamicField.primaryKey("id", "BIGINT"))
                                                 .addField(DynamicField.of("amount", numeric))
                                                 .build();
            SqlRequest request = renderer().update(numericForm,
                                                   Map.of("amount", UpdateDelta.increment(1)),
                                                   ConditionGroup.and().where("id", "=", 1L).build());
            assertEquals("update Metrics set amount = amount + ? where id = ?", request.sql());
        }

        for (String nonNumeric : List.of("INTERVAL DAY TO SECOND", "POINT", "PRINTABLE")) {
            DynamicForm nonNumericForm = DynamicForm.builder("metric", "Metrics")
                                                    .addField(DynamicField.primaryKey("id", "BIGINT"))
                                                    .addField(DynamicField.of("amount", nonNumeric))
                                                    .build();
            assertThrows(IllegalArgumentException.class,
                         () -> renderer().update(nonNumericForm,
                                                 Map.of("amount", UpdateDelta.increment(1)),
                                                 ConditionGroup.and().where("id", "=", 1L).build()));
        }
    }

    /**
     * PostgreSQL 会把没有双引号的名字折成小写。DDL 和 DML 必须共用同一套引用规则，否则大写动态表名
     * 虽然能建出来，后续 CRUD 却会访问另一张小写表。
     */
    @Test
    void usesPostgresqlIdentifierRulesForCrudAndConditions() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditionRenderer(), RdbDialect.postgresql());

        SqlRequest insert = renderer.insert(form(), orderedMap("id", "u1", "name", "王"));
        SqlRequest select = renderer.select(form(),
                                            ConditionGroup.and()
                                                          .where("name", "=", "王")
                                                          .build());

        assertEquals("insert into \"Users\" (\"id\", \"name\") values (?, ?)", insert.sql());
        assertEquals("select \"id\", \"name\", \"age\" from \"Users\" where \"name\" = ?", select.sql());
    }

    /** 五种受支持方言必须使用各自安全标识符规则渲染同一忽略大小写 LIKE 语义。 */
    @Test
    void rendersCaseInsensitiveLikeAcrossSupportedDialects() {
        assertEquals("select id, name, age from Users where lower(name) like lower(?)",
                     caseInsensitiveSelect(RdbDialect.h2()).sql());
        assertEquals("select `id`, `name`, `age` from `Users` where lower(`name`) like lower(?)",
                     caseInsensitiveSelect(RdbDialect.mysql()).sql());
        assertEquals("select \"id\", \"name\", \"age\" from \"Users\" where lower(\"name\") like lower(?)",
                     caseInsensitiveSelect(RdbDialect.postgresql()).sql());
        assertEquals("select \"id\", \"name\", \"age\" from \"Users\" where lower(\"name\") like lower(?)",
                     caseInsensitiveSelect(RdbDialect.oracle()).sql());
        assertEquals("select [id], [name], [age] from [Users] where lower([name]) like lower(?)",
                     caseInsensitiveSelect(RdbDialect.sqlServer()).sql());
        assertEquals(List.of("%Alice%"), caseInsensitiveSelect(RdbDialect.postgresql()).parameters());
    }

    @Test
    void rendersWriteParametersThroughValueCodecs() {
        SqlRequest insert = renderer().insert(form(),
                                              orderedMap("id", 1L, "name", Status.ACTIVE));
        SqlRequest update = renderer().update(form(),
                                              orderedMap("name", Status.DISABLED),
                                              ConditionGroup.and()
                                                            .where("id", "=", 1L)
                                                            .build());

        assertEquals(List.of(1L, "ACTIVE"), insert.parameters());
        assertEquals(List.of("DISABLED", 1L), update.parameters());
    }

    @Test
    void preservesOffsetTimeForNativeDialectsAndUsesTextForFallbackDialects() {
        DynamicForm form = DynamicForm.builder("schedule", "Schedules")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("meeting_time", "OFFSET_TIME"))
                                      .build();
        OffsetTime value = OffsetTime.parse("13:40:00+08:00");

        SqlRequest postgresql = FormDataSqlRenderer.create(conditionRenderer(), RdbDialect.postgresql())
                                                   .insert(form, orderedMap("id", 1L, "meeting_time", value));
        SqlRequest mysql = FormDataSqlRenderer.create(conditionRenderer(), RdbDialect.mysql())
                                              .insert(form, orderedMap("id", 1L, "meeting_time", value));
        BatchWriteRequest mysqlBatch = FormDataSqlRenderer.create(conditionRenderer(), RdbDialect.mysql())
                                                          .insertBatch(form,
                                                                       List.of(orderedMap("id", 1L,
                                                                                          "meeting_time", value),
                                                                               orderedMap("id", 2L,
                                                                                          "meeting_time", value)));

        assertEquals(value, postgresql.parameters().get(1));
        assertEquals("13:40+08:00", mysql.parameters().get(1));
        assertEquals("13:40+08:00", parameterRows(mysqlBatch).get(1).get(1));
    }

    @Test
    void rendersJsonStringFieldAsPlainBoundParameter() {
        DynamicForm form = DynamicForm.builder("profileForm", "Profiles")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("profile", "JSON"))
                                      .build();
        String json = "{\"name\":\"Alice\"}";
        RdbDialect plainJsonDialect = RdbDialect.of("plain-json-test",
                                                    SchemaDialect.standard(),
                                                    PaginationDialect.limitOffset(),
                                                    UpsertDialect.h2());
        FormDataSqlRenderer plainJsonRenderer = FormDataSqlRenderer.create(conditionRenderer(), plainJsonDialect);

        SqlRequest insert = plainJsonRenderer.insert(form, orderedMap("id", 1L, "profile", json));
        SqlRequest select = plainJsonRenderer.select(form,
                                                     ConditionGroup.and()
                                                                   .where("profile", "=", json)
                                                                   .build());

        assertEquals("insert into Profiles (id, profile) values (?, ?)", insert.sql());
        assertEquals(List.of(1L, json), insert.parameters());
        assertEquals("select id, profile from Profiles where profile = ?", select.sql());
        assertEquals(List.of(json), select.parameters());
    }

    @Test
    void serializesJsonObjectForSingleAndBatchWrites() {
        DynamicForm form = DynamicForm.builder("profileForm", "Profiles")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("profile", "JSON"))
                                      .build();
        Map<String, Object> profile = orderedMap("name", "Alice", "enabled", true);

        SqlRequest insert = renderer().insert(form, orderedMap("id", 1L, "profile", profile));
        BatchWriteRequest batch = renderer().insertBatch(
                form,
                List.of(orderedMap("id", 2L, "profile", profile)));

        assertEquals(List.of(1L, "{\"name\":\"Alice\",\"enabled\":true}"), insert.parameters());
        assertEquals(List.of(List.of(2L, "{\"name\":\"Alice\",\"enabled\":true}")),
                     parameterRows(batch));
    }

    @Test
    void rendersPostgresqlJsonWritesWithJsonbCast() {
        DynamicForm form = DynamicForm.builder("profileForm", "Profiles")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("profile", "JSON"))
                                      .build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditionRenderer(), RdbDialect.postgresql());

        SqlRequest insert = renderer.insert(form,
                                            orderedMap("id", 1L,
                                                       "profile", orderedMap("name", "Alice")));
        BatchWriteRequest upsert = renderer.upsertBatch(
                form,
                List.of(orderedMap("id", 1L, "profile", orderedMap("name", "Bob"))));

        assertEquals("insert into \"Profiles\" (\"id\", \"profile\") values (?, cast(? as jsonb))", insert.sql());
        assertEquals("insert into \"Profiles\" (\"id\", \"profile\") values (?, cast(? as jsonb)) "
                             + "on conflict (\"id\") do update set \"profile\" = excluded.\"profile\"",
                     upsert.sql());
    }

    @Test
    void rendersH2JsonWritesWithFormatJson() {
        DynamicForm form = DynamicForm.builder("profileForm", "Profiles")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("profile", "JSON"))
                                      .build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditionRenderer(), RdbDialect.h2());

        SqlRequest insert = renderer.insert(form,
                                            orderedMap("id", 1L,
                                                       "profile", orderedMap("name", "Alice")));
        BatchWriteRequest upsert = renderer.upsertBatch(
                form,
                List.of(orderedMap("id", 1L, "profile", orderedMap("name", "Bob"))));

        assertEquals("insert into Profiles (id, profile) values (?, ? format json)", insert.sql());
        assertEquals("merge into Profiles (id, profile) key (id) values (?, ? format json)", upsert.sql());
    }

    @Test
    void rejectsInvalidJsonBeforeRenderingWriteSql() {
        DynamicForm form = DynamicForm.builder("profileForm", "Profiles")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("profile", "JSON"))
                                      .build();

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> renderer().insert(form, orderedMap("id", 1L, "profile", "{broken")));

        assertEquals("json value cannot be serialized", error.getMessage());
    }

    @Test
    void normalizesClobAndRejectsInvalidBlobBeforeRenderingSql() {
        DynamicForm form = DynamicForm.builder("attachmentForm", "Attachments")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("payload", "BLOB"))
                                      .addField(DynamicField.of("content", "CLOB"))
                                      .build();

        SqlRequest insert = renderer().insert(form,
                                              orderedMap("id", 1L,
                                                         "payload", new byte[]{1, 2},
                                                         "content", new StringBuilder("  text  ")));
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> renderer().insert(form, orderedMap("id", 2L, "payload", "not binary")));

        assertArrayEquals(new byte[]{1, 2}, (byte[]) insert.parameters().get(1));
        assertEquals("  text  ", insert.parameters().get(2));
        assertEquals("binary large object must be byte[] or ByteBuffer", error.getMessage());
    }

    /**
     * 验证批量 insert 只生成一份 SQL，并按首行列顺序整理每一行参数。
     */
    @Test
    void rendersBatchInsertWithSharedColumnLayout() {
        BatchWriteRequest request = renderer().insertBatch(form(),
                                                         List.of(orderedMap("id", "u1", "name", "王"),
                                                                 orderedMap("name", "李", "id", "u2")));

        assertEquals("insert into Users (id, name) values (?, ?)", request.sql());
        assertEquals(List.of(List.of("u1", "王"), List.of("u2", "李")), parameterRows(request));
    }

    @Test
    void rendersBatchWriteParametersThroughValueCodecs() {
        BatchWriteRequest insert = renderer().insertBatch(form(),
                                                        List.of(orderedMap("id", 1L, "name", Status.ACTIVE),
                                                                orderedMap("name", Status.DISABLED, "id", 2L)));
        BatchWriteRequest upsert = FormDataSqlRenderer.create(conditionRenderer(), RdbDialect.h2())
                                                    .upsertBatch(form(),
                                                                 List.of(orderedMap("id", 3L,
                                                                                    "name", Status.ACTIVE)));

        assertEquals(List.of(List.of(1L, "ACTIVE"), List.of(2L, "DISABLED")), parameterRows(insert));
        assertEquals(List.of(List.of(3L, "ACTIVE")), parameterRows(upsert));
    }

    @Test
    void customValueCodecIsSharedByConditionsAndSingleOrBatchWrites() {
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(moneyCodec());
        SqlRenderer conditionRenderer = SqlRenderer.builder()
                                                    .valueCodecs(codecs)
                                                    .addTerm(SqlTermHandler.equalsTo())
                                                    .build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditionRenderer, RdbDialect.h2());
        Money value = new Money("12.30");

        SqlRequest insert = renderer.insert(form(), orderedMap("id", 1L, "name", value));
        SqlRequest select = renderer.select(form(),
                                            ConditionGroup.and()
                                                          .where("name", "=", value)
                                                          .build());
        BatchWriteRequest batch = renderer.insertBatch(form(),
                                                      List.of(orderedMap("id", 2L, "name", value)));

        assertEquals(List.of(1L, "CNY:12.30"), insert.parameters());
        assertEquals(List.of("CNY:12.30"), select.parameters());
        assertEquals(List.of(List.of(2L, "CNY:12.30")), parameterRows(batch));
    }

    /** 验证应用 codec 的首匹配优先级不会被 VARCHAR 的 UUID 默认文本回退抢先覆盖。 */
    @Test
    void applicationCodecOverridesDefaultUuidTextFallback() {
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == UUID.class;
            }

            @Override
            public Object write(Object value) {
                return "APP:" + value;
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                return UUID.fromString(value.toString().replaceFirst("^APP:", ""));
            }
        });
        SqlRenderer conditions = SqlRenderer.builder()
                                            .valueCodecs(codecs)
                                            .addTerm(SqlTermHandler.equalsTo())
                                            .build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, RdbDialect.h2());
        UUID value = UUID.fromString("9dd5fd17-81df-49a9-87db-d2ae09f5b5ae");

        SqlRequest insert = renderer.insert(form(), orderedMap("id", 1L, "name", value));
        SqlRequest select = renderer.select(form(), ConditionGroup.and(conditions.terms())
                                                                   .where("name", "=", value)
                                                                   .build());
        BatchWriteRequest batch = renderer.insertBatch(
                form(), List.of(orderedMap("id", 2L, "name", value)));

        String encoded = "APP:" + value;
        assertEquals(List.of(1L, encoded), insert.parameters());
        assertEquals(List.of(encoded), select.parameters());
        assertEquals(List.of(List.of(2L, encoded)), parameterRows(batch));
    }

    /**
     * 验证 H2 upsert 复用批量参数布局，SQL 用 merge 写法。
     */
    @Test
    void rendersH2BatchUpsertWithSharedColumnLayout() {
        BatchWriteRequest request = FormDataSqlRenderer.create(conditionRenderer(), RdbDialect.h2())
                                                     .upsertBatch(form(),
                                                                  List.of(orderedMap("id", "u1", "name", "王"),
                                                                          orderedMap("name", "李", "id", "u2")));

        assertEquals("merge into Users (id, name) key (id) values (?, ?)", request.sql());
        assertEquals(List.of(List.of("u1", "王"), List.of("u2", "李")), parameterRows(request));
    }

    /**
     * 验证 MySQL upsert 使用 on duplicate key update。
     */
    @Test
    void rendersMysqlBatchUpsert() {
        BatchWriteRequest request = FormDataSqlRenderer.create(conditionRenderer(), RdbDialect.mysql())
                                                     .upsertBatch(form(),
                                                                  List.of(orderedMap("id", "u1", "name", "王",
                                                                                     "age", 18)));

        assertEquals("insert into `Users` (`id`, `name`, `age`) values (?, ?, ?) "
                             + "on duplicate key update `name` = values(`name`), `age` = values(`age`)",
                     request.sql());
        assertEquals(List.of(List.of("u1", "王", 18)), parameterRows(request));
    }

    /**
     * 验证 PostgreSQL upsert 使用 on conflict，冲突键来自动态表单主键字段。
     */
    @Test
    void rendersPostgresqlBatchUpsert() {
        BatchWriteRequest request = FormDataSqlRenderer.create(conditionRenderer(), RdbDialect.postgresql())
                                                     .upsertBatch(form(),
                                                                  List.of(orderedMap("id", "u1", "name", "王",
                                                                                     "age", 18)));

        assertEquals("insert into \"Users\" (\"id\", \"name\", \"age\") values (?, ?, ?) "
                             + "on conflict (\"id\") do update set \"name\" = excluded.\"name\", "
                             + "\"age\" = excluded.\"age\"",
                     request.sql());
        assertEquals(List.of(List.of("u1", "王", 18)), parameterRows(request));
    }

    /**
     * 没有主键就不知道按什么判断冲突，直接拒绝。
     */
    @Test
    void rejectsBatchUpsertWithoutPrimaryKeyInSubmittedValues() {
        assertThrows(IllegalArgumentException.class,
                     () -> FormDataSqlRenderer.create(conditionRenderer(), RdbDialect.h2())
                                              .upsertBatch(form(), List.of(orderedMap("name", "王"))));
    }

    /**
     * 验证批量计划只编译一次 SQL，后续行只按首行字段布局整理参数。
     */
    @Test
    void compilesOneInsertPlanAndMapsLaterRowsByLayout() {
        BatchInsertPlan plan = renderer().insertPlan(form(), orderedMap("id", 1L, "name", "A", "age", 20));
        Object[] secondRow = plan.parameters(orderedMap("age", 21, "name", "B", "id", 2L), 1);
        BatchWriteRequest request = plan.request(Flux.<Object[]>just(secondRow), BatchWriteOptions.atomic(2));

        assertEquals("insert into Users (id, name, age) values (?, ?, ?)", plan.sql());
        assertArrayEquals(new Object[]{2L, "B", 21}, secondRow);
        assertEquals(3, request.parameterCount());
        assertEquals(List.of(Long.class, String.class, Integer.class), request.parameterTypes());
        assertEquals(BatchWriteOptions.Mode.ATOMIC, request.options().mode());
        assertThrows(IllegalArgumentException.class,
                     () -> plan.parameters(orderedMap("id", 3L, "name", "C"), 2));
    }

    /**
     * 整批某列都是 null 时，R2DBC 仍然要靠动态字段类型知道 bindNull 用什么 Java 类型。
     */
    @Test
    void compilesParameterTypesFromDynamicFieldDefinitionsWhenValuesAreNull() {
        DynamicForm form = DynamicForm.builder("attachmentForm", "Attachments")
                                      .addField(DynamicField.primaryKey("id", "VARCHAR"))
                                      .addField(DynamicField.of("payload", "BLOB"))
                                      .addField(DynamicField.of("content", "CLOB"))
                                      .addField(DynamicField.of("amount", "DECIMAL"))
                                      .addField(DynamicField.of("enabled", "BOOLEAN"))
                                      .addField(DynamicField.of("createdAt", "TIMESTAMP"))
                                      .addField(DynamicField.of("birthday", "DATE"))
                                      .addField(DynamicField.of("clock", "TIME"))
                                      .build();

        BatchInsertPlan plan = renderer().insertPlan(form,
                                                     orderedMap("id", "a1",
                                                                   "payload", null,
                                                                   "content", null,
                                                                   "amount", null,
                                                                "enabled", null,
                                                                "createdAt", null,
                                                                "birthday", null,
                                                                "clock", null));
        BatchWriteRequest request = plan.request(Flux.empty(), BatchWriteOptions.atomic(1));

        assertEquals(List.of(String.class,
                             byte[].class,
                             String.class,
                             BigDecimal.class,
                             Boolean.class,
                             LocalDateTime.class,
                             LocalDate.class,
                             LocalTime.class),
                     request.parameterTypes());
    }

    /**
     * 验证同一批次字段集合不一致时直接拒绝，避免参数绑到错误列。
     */
    @Test
    void rejectsBatchRowsWithDifferentFields() {
        assertThrows(IllegalArgumentException.class,
                     () -> renderer().insertBatch(form(),
                                                  List.of(orderedMap("id", "u1", "name", "王"),
                                                          orderedMap("id", "u2", "age", 18))));
    }

    /**
     * 验证首行里大小写不同但实际指向同一字段的键会被拒绝。
     */
    @Test
    void rejectsDuplicateNormalizedFieldsInFirstBatchRow() {
        assertThrows(IllegalArgumentException.class,
                     () -> renderer().insertBatch(form(),
                                                  List.of(orderedMap("id", "u1", "ID", "u2"))));
    }

    /**
     * 验证 select SQL 复用动态条件渲染能力。
     */
    @Test
    void rendersSelectSqlWithWhereCondition() {
        SqlRequest request = renderer().select(form(),
                                               ConditionGroup.and()
                                                             .where("id", "=", "u1")
                                                             .build());

        assertEquals("select id, name, age from Users where id = ?", request.sql());
        assertEquals(List.of("u1"), request.parameters());
    }

    /**
     * 验证三列 keyset 游标的 SQL 占位符和参数顺序保持稳定。
     */
    @Test
    void preservesThreeSortCursorSqlAndParameterOrder() {
        DynamicForm cursorForm = DynamicForm.builder("events", "Events")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("createdAt", "BIGINT").withNullable(false))
                .addField(DynamicField.of("sequence", "BIGINT").withNullable(false))
                .build();

        SqlRequest request = renderer().select(
                cursorForm,
                ConditionGroup.and().build(),
                CursorPageQuery.after(3, List.of(30L, 20L, 10L),
                                      CursorSort.asc("createdAt"),
                                      CursorSort.desc("sequence"),
                                      CursorSort.asc("id")));

        assertEquals("select id, createdAt, sequence from Events where (createdAt > ? or "
                             + "(createdAt = ? and sequence < ?) or "
                             + "(createdAt = ? and sequence = ? and id > ?)) "
                             + "order by createdAt asc, sequence desc, id asc limit ? offset ?",
                     request.sql());
        assertEquals(List.of(30L, 30L, 20L, 30L, 20L, 10L, 4, 0L), request.parameters());
    }

    /**
     * 验证分页 select SQL 会追加排序、limit 和 offset，并且 count SQL 复用相同 where。
     */
    @Test
    void rendersPagedSelectAndCountSqlWithWhereCondition() {
        ConditionGroup where = ConditionGroup.and()
                                             .where("name", "=", "王")
                                             .build();
        PageQuery page = PageQuery.of(2, 20, PageSort.desc("age"), PageSort.asc("id"));
        FormDataSqlRenderer renderer = renderer();

        SqlRequest select = renderer.select(form(), where, page);
        SqlRequest count = renderer.count(form(), where);

        assertEquals("select id, name, age from Users where name = ? order by age desc, id asc limit ? offset ?",
                     select.sql());
        assertEquals(List.of("王", 20, 20L), select.parameters());
        assertEquals("select count(*) as total from Users where name = ?", count.sql());
        assertEquals(List.of("王"), count.parameters());
    }

    /**
     * 验证动态表单数据渲染器可以替换分页方言，不把 limit/offset 写死在核心流程。
     */
    @Test
    void rendersPagedSelectWithConfiguredPaginationDialect() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder()
                                                                             .addTerm(SqlTermHandler.equalsTo())
                                                                             .build(),
                                                                  RdbDialect.of("offset-fetch-test",
                                                                                SchemaDialect.standard(),
                                                                                PaginationDialect.offsetFetch(),
                                                                                UpsertDialect.h2()));

        SqlRequest request = renderer.select(form(),
                                             ConditionGroup.and()
                                                           .where("name", "=", "王")
                                                           .build(),
                                             PageQuery.of(2, 20, PageSort.asc("id")));

        assertEquals("select id, name, age from Users where name = ? order by id asc offset ? rows fetch next ? rows only",
                     request.sql());
        assertEquals(List.of("王", 20L, 20), request.parameters());
    }

    /**
     * 验证 update SQL 会先绑定 set 参数，再绑定 where 参数。
     */
    @Test
    void rendersUpdateSqlWithSetValuesAndWhereParameters() {
        SqlRequest request = renderer().update(form(),
                                               orderedMap("name", "王", "age", 18),
                                               ConditionGroup.and()
                                                             .where("id", "=", "u1")
                                                             .build());

        assertEquals("update Users set name = ?, age = ? where id = ?", request.sql());
        assertEquals(List.of("王", 18, "u1"), request.parameters());
    }

    /**
     * 验证 update 的归一化重复字段错误不回显调用方提供的超长原始键。
     */
    @Test
    void rejectsDuplicateNormalizedUpdateFieldsWithoutEchoingRawCallerKey() {
        String secretRawFieldName = " ".repeat(512) + "name" + " ".repeat(512);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> renderer().update(form(),
                                        orderedMap("name", "first", secretRawFieldName, "second"),
                                        ConditionGroup.and().where("id", "=", "u1").build()));

        assertEquals("duplicate normalized dynamic update field", error.getMessage());
        assertFalse(error.getMessage().contains(secretRawFieldName));
    }

    @Test
    void rendersOptimisticLockedUpdateWithVersionIncrement() {
        SqlRequest request = renderer().update(versionedForm(),
                                               orderedMap("name", "王"),
                                               ConditionGroup.and()
                                                             .where("id", "=", "u1")
                                                             .build(),
                                               OptimisticLockOptions.increment("version", 3));

        assertEquals("update Users set name = ?, version = version + 1 where id = ? and version = ?",
                     request.sql());
        assertEquals(List.of("王", "u1", 3), request.parameters());
    }

    @Test
    void rendersOptimisticLockedUpdateWithAssignedVersion() {
        LocalDateTime nextUpdatedAt = LocalDateTime.of(2026, 7, 30, 16, 0);
        SqlRequest request = renderer().update(versionedForm(),
                                               orderedMap("name", "王"),
                                               ConditionGroup.and()
                                                             .where("id", "=", "u1")
                                                             .build(),
                                               OptimisticLockOptions.assign("updatedAt",
                                                                            "2026-07-30T15:00:00",
                                                                            nextUpdatedAt));

        assertEquals("update Users set name = ?, updatedAt = ? where id = ? and updatedAt = ?", request.sql());
        assertEquals(List.of("王", nextUpdatedAt, "u1", "2026-07-30T15:00:00"), request.parameters());
    }

    /** 验证二进制显式版本值在构造和访问后都不会反向改写后续 SQL 绑定。 */
    @Test
    void snapshotsBinaryAssignedOptimisticLockValues() {
        byte[] expected = {1, 2};
        byte[] next = {3, 4};
        OptimisticLockOptions lock = OptimisticLockOptions.assign("version", expected, next);
        expected[0] = 9;
        next[0] = 9;
        ((byte[]) lock.expectedValue())[1] = 8;
        ((byte[]) lock.nextValue())[1] = 8;
        DynamicForm form = DynamicForm.builder("binaryVersionForm", "Documents")
                                      .addField(DynamicField.primaryKey("id", "VARCHAR"))
                                      .addField(DynamicField.of("name", "VARCHAR"))
                                      .addField(DynamicField.of("version", "BLOB"))
                                      .build();

        SqlRequest request = renderer().update(form,
                                               orderedMap("name", "updated"),
                                               ConditionGroup.and().where("id", "=", "doc-1").build(),
                                               lock);

        assertArrayEquals(new byte[]{3, 4}, (byte[]) request.parameters().get(1));
        assertArrayEquals(new byte[]{1, 2}, (byte[]) request.parameters().get(3));
    }

    /** 乐观锁显式版本值的完整数组图必须在构造和访问两端隔离。 */
    @Test
    void snapshotsNestedArrayGraphsForAssignedOptimisticLockValues() {
        byte[][] expected = new byte[][]{{1, 2}};
        byte[][] next = new byte[][]{{3, 4}};
        OptimisticLockOptions lock = OptimisticLockOptions.assign("version", expected, next);

        expected[0][0] = 9;
        next[0][0] = 9;
        ((byte[][]) lock.expectedValue())[0][1] = 8;
        ((byte[][]) lock.nextValue())[0][1] = 8;

        assertArrayEquals(new byte[]{1, 2}, ((byte[][]) lock.expectedValue())[0]);
        assertArrayEquals(new byte[]{3, 4}, ((byte[][]) lock.nextValue())[0]);
    }

    /** 随机密文不能承担数据库原子比较或递增所需的乐观锁职责。 */
    @Test
    void rejectsEncryptedFieldsAsDynamicOptimisticLocks() {
        DynamicForm encrypted = DynamicForm.builder("encryptedVersionForm", "Documents")
                                           .addField(DynamicField.primaryKey("id", "VARCHAR"))
                                           .addField(DynamicField.of("version", "VARCHAR"))
                                           .encrypted("version", EncryptedFieldDefinition.builder().build())
                                           .build();
        ConditionGroup where = ConditionGroup.and().where("id", "=", "doc-1").build();

        IllegalArgumentException encryptedError = assertThrows(
                IllegalArgumentException.class,
                () -> renderer().update(encrypted, orderedMap("id", "doc-1"), where,
                                        OptimisticLockOptions.increment("version", "v1")));
        IllegalArgumentException encryptedDeleteError = assertThrows(
                IllegalArgumentException.class,
                () -> renderer().delete(encrypted, where,
                                        OptimisticLockOptions.increment("version", "v1")));

        assertEquals("optimistic lock field must not be encrypted", encryptedError.getMessage());
        assertEquals("optimistic lock field must not be encrypted", encryptedDeleteError.getMessage());
    }

    /**
     * 验证 delete SQL 必须带 where 条件，避免误删全表。
     */
    @Test
    void rendersDeleteSqlAndRejectsEmptyWhereCondition() {
        FormDataSqlRenderer renderer = renderer();

        SqlRequest request = renderer.delete(form(),
                                             ConditionGroup.and()
                                                           .where("id", "=", "u1")
                                                           .build());

        assertEquals("delete from Users where id = ?", request.sql());
        assertEquals(List.of("u1"), request.parameters());
        assertThrows(IllegalArgumentException.class, () -> renderer.delete(form(), ConditionGroup.and().build()));
    }

    @Test
    void rendersOptimisticLockedDeleteWithVersionCondition() {
        SqlRequest request = renderer().delete(versionedForm(),
                                               ConditionGroup.and()
                                                             .where("id", "=", "u1")
                                                             .build(),
                                               OptimisticLockOptions.increment("version", 3));

        assertEquals("delete from Users where id = ? and version = ?", request.sql());
        assertEquals(List.of("u1", 3), request.parameters());
    }

    /**
     * 验证数据 Map 不能提交表单中不存在的字段。
     */
    @Test
    void rejectsUnknownDynamicFormField() {
        String secret = "tenant-secret-field";

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> renderer().insert(form(), orderedMap(secret, "x")));

        assertFalse(error.getMessage().contains(secret));
    }

    @Test
    void rejectsUnsafeDynamicFormIdentifiers() {
        DynamicForm unsafeTable = DynamicForm.builder("unsafeForm", "Users; drop table Users")
                                            .addField(DynamicField.of("name", "VARCHAR"))
                                            .build();
        DynamicForm unsafeField = DynamicForm.builder("unsafeForm", "Users")
                                            .addField(DynamicField.of("name or 1=1", "VARCHAR"))
                                            .build();

        assertThrows(IllegalArgumentException.class,
                     () -> renderer().insert(unsafeTable, orderedMap("name", "A")));
        assertThrows(IllegalArgumentException.class,
                     () -> renderer().insert(unsafeField, orderedMap("name or 1=1", "A")));
    }

    @Test
    void compilesTypedArrayParametersForBatchWrites() {
        DynamicForm form = DynamicForm.builder("arrayForm", "ArrayRecords")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("tags", "VARCHAR[]"))
                                      .build();
        BatchInsertPlan plan = renderer().insertPlan(form, orderedMap("id", 1L, "tags", List.of("a", "b")));

        assertEquals(List.of(Long.class, String[].class), plan.parameterTypes());
        Object[] parameters = plan.parameters(orderedMap("id", 2L, "tags", List.of("c", "d")), 1);
        assertArrayEquals(new String[]{"c", "d"}, (String[]) parameters[1]);
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(conditionRenderer(), RdbDialect.h2());
    }

    private static SqlRequest caseInsensitiveSelect(RdbDialect dialect) {
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms().build();
        return FormDataSqlRenderer.create(conditions, dialect)
                                  .select(form(), ConditionGroup.and(conditions.terms())
                                                                .where("name", "like-ignore-case", "%Alice%")
                                                                .build());
    }

    /** 把测试用的小批量参数流收集起来，生产执行仍由批量 writer 按分片消费。 */
    private static List<List<Object>> parameterRows(BatchWriteRequest request) {
        return Flux.from(request.rows()).map(Arrays::asList).collectList().block();
    }

    private static SqlRenderer conditionRenderer() {
        return SqlRenderer.builder()
                          .addTerm(SqlTermHandler.equalsTo())
                          .build();
    }

    private static DynamicForm form() {
        return DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .addField(DynamicField.of("age", "INTEGER"))
                          .build();
    }

    private static DynamicForm versionedForm() {
        return DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .addField(DynamicField.of("version", "INTEGER"))
                          .addField(DynamicField.of("updatedAt", "TIMESTAMP"))
                          .build();
    }

    private static Map<String, Object> orderedMap(Object... pairs) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            values.put((String) pairs[i], pairs[i + 1]);
        }
        return values;
    }

    private enum Status {
        ACTIVE,
        DISABLED
    }

    private static ValueCodec moneyCodec() {
        return new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == Money.class;
            }

            @Override
            public Object write(Object value) {
                return "CNY:" + ((Money) value).amount();
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                return new Money(value.toString());
            }
        };
    }

    private record Money(String amount) {
    }
}
