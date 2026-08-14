package com.flying.orm.core.sql.render;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionValueException;
import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import org.junit.jupiter.api.Test;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证 SQL 渲染器输出参数化 SQL，业务参数必须进入参数列表而不是拼接进 SQL 文本。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
class SqlRendererTest {

    /** 直接加入条件树的集合值也必须在构造时快照，避免外部修改改变 SQL 与参数顺序。 */
    @Test
    void snapshotsMutableTermValuesBeforeRendering() {
        ArrayList<String> ids = new ArrayList<>(List.of("u1"));
        String[] statuses = {"active"};
        TermCondition statusTerm = TermCondition.of("status", "in", statuses);
        ConditionGroup group = ConditionGroup.and()
                                             .add(TermCondition.of("id", "in", ids))
                                             .add(statusTerm)
                                             .build();
        ids.add("u2");
        statuses[0] = "disabled";
        ((String[]) statusTerm.value())[0] = "archived";

        SqlFragment fragment = SqlRenderer.builder()
                                          .addDefaultTerms()
                                          .build()
                                          .renderWhere(group);

        assertEquals("id in (?) and status in (?)", fragment.sql());
        assertEquals(List.of("u1", "active"), fragment.parameters());
    }

    /** 标准范围条件同样在构造期冻结集合，并拒绝超过公开上限的直接 AST 输入。 */
    @Test
    void snapshotsStandardRangeCollectionsAndRejectsOversizedCollections() {
        ArrayList<Integer> range = new ArrayList<>(List.of(1, 9));
        TermCondition condition = TermCondition.of("score", "between", range);
        range.set(1, 10);

        assertEquals(List.of(1, 9), condition.value());

        ArrayList<Integer> oversized = new ArrayList<>();
        for (int index = 0; index <= 1_000; index++) {
            oversized.add(index);
        }
        assertThrows(ConditionValueException.class, () -> TermCondition.of("score", "between", oversized));
    }

    /** 标准多值 term 对非 Collection 的 Iterable 也必须在发布 AST 时冻结。 */
    @Test
    void snapshotsStandardIterableTermsBeforeRendering() {
        ArrayList<String> source = new ArrayList<>(List.of("u1"));
        Iterable<String> values = source::iterator;
        TermCondition term = TermCondition.of("id", "in", values);
        source.add("u2");

        SqlFragment fragment = SqlRenderer.builder()
                                          .addDefaultTerms()
                                          .build()
                                          .renderWhere(ConditionGroup.and().add(term).build());

        assertEquals("id in (?)", fragment.sql());
        assertEquals(List.of("u1"), fragment.parameters());
    }

    /** 标准多值 term 的数组元素同样属于已发布 AST，不能通过源数组或 accessor 改写。 */
    @Test
    void snapshotsBinaryElementsInsideStandardCollectionTerms() {
        byte[] payload = {7, 8};
        TermCondition term = TermCondition.of("payload", "in", List.of(payload));
        payload[0] = 9;

        byte[] exposed = (byte[]) ((List<?>) term.value()).getFirst();
        assertArrayEquals(new byte[]{7, 8}, exposed);
        exposed[1] = 6;

        SqlFragment fragment = SqlRenderer.builder()
                                          .addDefaultTerms()
                                          .build()
                                          .renderWhere(ConditionGroup.and().add(term).build());
        assertArrayEquals(new byte[]{7, 8}, (byte[]) fragment.parameters().getFirst());
    }

    /** 标准多值中的嵌套数组图也必须隔离，同时保留共享引用与自环而不递归失控。 */
    @Test
    void snapshotsNestedArrayGraphsInsideStandardTerms() {
        byte[] shared = {3, 4};
        Object[] nested = {shared, shared};
        Object[] cycle = new Object[1];
        cycle[0] = cycle;
        TermCondition term = TermCondition.of("payload", "in", new Object[]{nested, cycle});
        shared[0] = 9;

        Object[] first = (Object[]) term.value();
        Object[] firstNested = (Object[]) first[0];
        assertArrayEquals(new byte[]{3, 4}, (byte[]) firstNested[0]);
        assertSame(firstNested[0], firstNested[1]);
        assertNotSame(shared, firstNested[0]);
        assertSame(first[1], ((Object[]) first[1])[0]);

        ((byte[]) firstNested[0])[1] = 8;
        Object[] second = (Object[]) term.value();
        assertArrayEquals(new byte[]{3, 4}, (byte[]) ((Object[]) second[0])[0]);
        assertSame(second[1], ((Object[]) second[1])[0]);
    }

    /** 二进制数组是单个驱动参数，不得被集合项上限误判；同时仍要保存独立快照。 */
    @Test
    void preservesLargeBinaryArrayAsDirectScalarTerm() {
        byte[] payload = new byte[1_024];
        payload[0] = 7;
        TermCondition term = TermCondition.of("payload", "=", payload);
        payload[0] = 9;

        SqlFragment fragment = SqlRenderer.builder()
                                          .addDefaultTerms()
                                          .build()
                                          .renderWhere(ConditionGroup.and().add(term).build());
        byte[] bound = (byte[]) fragment.parameters().getFirst();

        assertEquals("payload = ?", fragment.sql());
        assertEquals(1_024, bound.length);
        assertEquals(7, bound[0]);
        assertFalse(bound == payload);
    }

    /**
     * 直接 AST 允许可信扩展把 Iterable 实现作为单个标量驱动值；快照逻辑不能把它误改写为集合参数。
     */
    @Test
    void preservesOpaqueIterableAsDirectScalarTerm() {
        OpaqueIterableValue payload = new OpaqueIterableValue();
        ValueCodec codec = new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == OpaqueIterableValue.class;
            }

            @Override
            public Object write(Object value) {
                return value;
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                return value;
            }
        };
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addTerm(SqlTermHandler.of("opaque-scalar",
                                                                      (term, context) -> SqlFragment.of(
                                                                              "payload = ?",
                                                                              context.parameter(term.value()))))
                                          .valueCodecs(ValueCodecRegistry.standard().withFirst(codec))
                                          .build();

        SqlFragment fragment = renderer.renderWhere(ConditionGroup.and()
                                                                   .add(TermCondition.of(
                                                                           "payload", "opaque-scalar", payload))
                                                                   .build());

        assertSame(payload, fragment.parameters().getFirst());
    }

    /**
     * 自定义 scalar driver value 即使实现 Collection，也不能被通用快照改写成 List，
     * 否则按其运行时类匹配的 codec 会在渲染前失效。
     */
    @Test
    void preservesOpaqueCollectionAsDirectScalarTerm() {
        OpaqueCollectionValue payload = new OpaqueCollectionValue();
        ValueCodec codec = new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == OpaqueCollectionValue.class;
            }

            @Override
            public Object write(Object value) {
                return value;
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                return value;
            }
        };
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addTerm(SqlTermHandler.of("opaque-collection",
                                                                      (term, context) -> SqlFragment.of(
                                                                              "payload = ?",
                                                                              context.parameter(term.value()))))
                                          .valueCodecs(ValueCodecRegistry.standard().withFirst(codec))
                                          .build();

        SqlFragment fragment = renderer.renderWhere(ConditionGroup.and()
                                                                   .add(TermCondition.of(
                                                                           "payload", "opaque-collection", payload))
                                                                   .build());

        assertSame(payload, fragment.parameters().getFirst());
    }

    @Test
    void rejectsStandardTermWithConflictingValueShape() {
        assertThrows(IllegalArgumentException.class,
                     () -> SqlRenderer.builder()
                                      .addTerm(SqlTermHandler.of("=",
                                                                    ConditionValueShape.COLLECTION,
                                                                    (term, context) -> SqlFragment.of("unsafe")))
                                      .build());
    }

    /**
     * 验证内置等值条件和业务自定义 term 都能渲染为参数化 SQL。
     */
    @Test
    void rendersParameterizedCustomTerm() {
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addTerm(SqlTermHandler.equalsTo())
                                          .addTerm(SqlTermHandler.of("user-in-org",
                                                                    (term, context) -> SqlFragment.of(
                                                                            "exists (select 1 from org_user ou where ou.user_id = "
                                                                                    + context.identifier(term.field())
                                                                                    + " and ou.org_id = ?)",
                                                                            term.value())))
                                          .build();

        SqlFragment fragment = renderer.renderWhere(ConditionGroup.and(renderer.terms())
                                                                   .where("status", "=", "enabled")
                                                                   .where("userId", "user-in-org", "org-1")
                                                                   .build());

        assertEquals("status = ? and exists (select 1 from org_user ou where ou.user_id = userId and ou.org_id = ?)",
                     fragment.sql());
        assertEquals(List.of("enabled", "org-1"), fragment.parameters());
        assertFalse(fragment.sql().contains("enabled"));
        assertFalse(fragment.sql().contains("org-1"));
        assertThrows(UnsupportedOperationException.class, () -> fragment.parameters().add("unsafe"));
    }

    @Test
    void rejectsUnsafeSqlIdentifiers() {
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addTerm(SqlTermHandler.equalsTo())
                                          .build();

        ConditionGroup unsafeWhere = ConditionGroup.and()
                                                   .where("status or 1=1", "=", "enabled")
                                                   .build();

        assertThrows(IllegalArgumentException.class, () -> renderer.identifier("Users; drop table Users"));
        assertThrows(IllegalArgumentException.class,
                     () -> SqlIdentifiers.requireProjection("count(*)", "test projection"));
        assertThrows(IllegalArgumentException.class, () -> renderer.renderWhere(unsafeWhere));
    }

    /** 非法标识符可能来自不可信输入，异常链不能回显其中的敏感文本或 SQL 片段。 */
    @Test
    void unsafeIdentifierFailureDoesNotExposeInput() {
        String secret = "users; password=secret-token";

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> SqlIdentifiers.requireIdentifier(secret, "query table"));

        assertFalse(error.getMessage().contains(secret));
        assertFalse(error.getMessage().contains("secret-token"));
    }

    /** 标识符校验的公开诊断标签也不能成为异常回显任意数据的通道。 */
    @Test
    void identifierValidationFailureDoesNotExposeDiagnosticLabel() {
        String secretLabel = "secret-label-" + "x".repeat(5_000);

        IllegalArgumentException identifierError = assertThrows(IllegalArgumentException.class,
                () -> SqlIdentifiers.requireIdentifier("not a plain identifier", secretLabel));
        IllegalArgumentException projectionError = assertThrows(IllegalArgumentException.class,
                () -> SqlIdentifiers.requireProjection("count(*)", secretLabel));

        assertEquals("sql identifier must be a plain identifier", identifierError.getMessage());
        assertEquals("sql projection must be a plain identifier", projectionError.getMessage());
    }

    @Test
    void rejectsImplicitNullAndRendersExplicitNullCondition() {
        assertThrows(IllegalArgumentException.class,
                     () -> ConditionGroup.and().where("deleted_at", "=", null));
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addDefaultTerms()
                                          .build();

        SqlFragment fragment = renderer.renderWhere(ConditionGroup.and()
                                                                   .whereNull("deleted_at")
                                                                   .build());

        assertEquals("deleted_at is null", fragment.sql());
        assertEquals(List.of(), fragment.parameters());
    }

    /**
     * 验证条件树可以独立渲染为 where 片段，供 update/delete 等命令复用。
     */
    @Test
    void rendersWhereConditionAsSqlFragment() {
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addTerm(SqlTermHandler.equalsTo())
                                          .build();

        SqlFragment fragment = renderer.renderWhere(ConditionGroup.and()
                                                                  .where("status", "=", "enabled")
                                                                  .build());

        assertEquals("status = ?", fragment.sql());
        assertEquals(List.of("enabled"), fragment.parameters());
    }

    /**
     * 验证关系存在型业务算子可以作为可复用 SPI 注册，业务侧仍然只表达结构化 where。
     */
    @Test
    void rendersRelationExistsTermForUserInOrgOperator() {
        SqlTermPackage relations = organizationRelations();
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addTermPackage(relations)
                                          .build();

        SqlFragment fragment = renderer.renderWhere(renderer.conditions()
                                                                  .where("userId", "user-in-org", "org-1")
                                                                  .build());

        assertEquals("exists (select 1 from org_user ou where ou.user_id = userId and ou.org_id = ?)",
                     fragment.sql());
        assertEquals(List.of("org-1"), fragment.parameters());
        assertFalse(fragment.sql().contains("org-1"));
    }

    /**
     * 验证关系存在型业务算子支持集合参数，并渲染为参数化 in 子句。
     */
    @Test
    void rendersRelationExistsTermWithCollectionValue() {
        SqlTermPackage relations = organizationRelations();
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addTermPackage(relations)
                                          .build();

        SqlFragment fragment = renderer.renderWhere(renderer.conditions()
                                                                  .where("userId",
                                                                         "user-in-org",
                                                                         List.of("org-1", "org-2"))
                                                                  .build());

        assertEquals("exists (select 1 from org_user ou where ou.user_id = userId and ou.org_id in (?, ?))",
                     fragment.sql());
        assertEquals(List.of("org-1", "org-2"), fragment.parameters());
    }

    /**
     * 验证关系不存在型业务算子可以表达排除关系，供数据权限和反向筛选复用。
     */
    @Test
    void rendersRelationNotExistsTerm() {
        SqlTermPackage relations = organizationRelations();
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addTermPackage(relations)
                                          .build();

        SqlFragment fragment = renderer.renderWhere(renderer.conditions()
                                                                  .where("userId",
                                                                         "user-not-in-org",
                                                                         new String[]{"org-1", "org-2"})
                                                                  .build());

        assertEquals("not exists (select 1 from org_user ou where ou.user_id = userId and ou.org_id in (?, ?))",
                     fragment.sql());
        assertEquals(List.of("org-1", "org-2"), fragment.parameters());
    }

    private static SqlTermPackage organizationRelations() {
        return RelationTermPackage.of("organization-relations",
                                      "org_user",
                                      "ou",
                                      "user_id",
                                      "org_id",
                                      "user-in-org",
                                      "user-not-in-org");
    }

    /**
     * 验证默认 SQL term 包覆盖常用条件，并保持参数化输出。
     */
    @Test
    void rendersWhereConditionWithDefaultTerms() {
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addDefaultTerms()
                                          .build();

        SqlFragment fragment = renderer.renderWhere(ConditionGroup.and()
                                                                  .where("age", ">", 18)
                                                                  .where("score", "<", 100)
                                                                  .where("created_at", ">=", "2026-07-01")
                                                                  .where("updated_at", "<=", "2026-07-31")
                                                                  .where("name", "like", "王%")
                                                                  .where("id", "in", List.of("u1", "u2"))
                                                                  .build());

        assertEquals("age > ? and score < ? and created_at >= ? and updated_at <= ? and name like ? and id in (?, ?)",
                     fragment.sql());
        assertEquals(List.of(18, 100, "2026-07-01", "2026-07-31", "王%", "u1", "u2"),
                     fragment.parameters());
    }

    /** 忽略大小写的 LIKE 必须同时折叠字段和绑定参数，不能在 Java 端改写调用方文本。 */
    @Test
    void rendersCaseInsensitiveLikeTermsWithBoundParameters() {
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addDefaultTerms()
                                          .build();

        SqlFragment fragment = renderer.renderWhere(ConditionGroup.and()
                                                                  .where("name",
                                                                         "like-ignore-case",
                                                                         "%Alice%")
                                                                  .where("email",
                                                                         "not-like-ignore-case",
                                                                         "%EXAMPLE.COM")
                                                                  .build());

        assertEquals("lower(name) like lower(?) and lower(email) not like lower(?)", fragment.sql());
        assertEquals(List.of("%Alice%", "%EXAMPLE.COM"), fragment.parameters());
        assertFalse(fragment.sql().contains("Alice"));
        assertFalse(fragment.sql().contains("EXAMPLE.COM"));
    }

    @Test
    void rendersConditionParametersThroughValueCodecs() {
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addDefaultTerms()
                                          .build();

        SqlFragment fragment = renderer.renderWhere(ConditionGroup.and()
                                                                  .where("status", "=", Status.ACTIVE)
                                                                  .where("nextStatus",
                                                                         "in",
                                                                         List.of(Status.ACTIVE, Status.DISABLED))
                                                                  .build());

        assertEquals("status = ? and nextStatus in (?, ?)", fragment.sql());
        assertEquals(List.of("ACTIVE", "ACTIVE", "DISABLED"), fragment.parameters());
    }

    /** 配置 codec 已生成的驱动对象必须原样绑定，同时公开 SqlFragment 入口继续使用标准 codec。 */
    @Test
    void preservesOpaqueConfiguredCodecValuesWithoutChangingDirectFragmentConversion() {
        OpaqueDriverValue opaque = new OpaqueDriverValue();
        ValueCodec codec = new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == OpaqueDomainValue.class;
            }

            @Override
            public Object write(Object value) {
                return opaque;
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                return value;
            }
        };
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addDefaultTerms()
                                          .valueCodecs(ValueCodecRegistry.standard().withFirst(codec))
                                          .build();

        SqlFragment rendered = renderer.renderWhere(renderer.conditions()
                                                             .where("payload", "=", new OpaqueDomainValue())
                                                             .build());
        SqlFragment direct = SqlFragment.of("?", Status.ACTIVE);

        assertEquals("payload = ?", rendered.sql());
        assertSame(opaque, rendered.parameters().getFirst());
        assertEquals(List.of("ACTIVE"), direct.parameters());
    }

    /** 配置 codec 输出即使被标准 codec 识别，也不得再次改写。 */
    @Test
    void preservesConfiguredCodecValuesRecognizedByStandardCodecs() {
        StringBuilder driverValue = new StringBuilder("driver-value");
        ValueCodec codec = new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == OpaqueDomainValue.class;
            }

            @Override
            public Object write(Object value) {
                return driverValue;
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                return value;
            }
        };
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addDefaultTerms()
                                          .valueCodecs(ValueCodecRegistry.standard().withFirst(codec))
                                          .build();

        SqlFragment rendered = renderer.renderWhere(renderer.conditions()
                                                             .where("payload", "=", new OpaqueDomainValue())
                                                             .build());

        SqlRenderer customRenderer = SqlRenderer.builder()
                                                .addTerm(SqlTermHandler.of(
                                                        "encoded-custom",
                                                        (term, context) -> SqlFragment.of(
                                                                "payload = ?",
                                                                context.parameter(term.value()))))
                                                .valueCodecs(ValueCodecRegistry.standard().withFirst(codec))
                                                .build();
        SqlFragment customRendered = customRenderer.renderWhere(
                customRenderer.conditions()
                              .where("payload", "encoded-custom", new OpaqueDomainValue())
                              .build());
        SqlRequest directRequest = new SqlRequest(
                "select ?", List.of(renderer.parameter(new OpaqueDomainValue())));

        assertSame(driverValue, rendered.parameters().getFirst());
        assertSame(driverValue, customRendered.parameters().getFirst());
        assertSame(driverValue, directRequest.parameters().getFirst());
    }

    @Test
    void rendersNegativeRangeAndNotNullTerms() {
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addDefaultTerms()
                                          .build();

        SqlFragment fragment = renderer.renderWhere(ConditionGroup.and()
                                                                  .where("status", "<>", "disabled")
                                                                  .where("id", "not-in", List.of(1, 2))
                                                                  .where("score", "between", List.of(60, 100))
                                                                  .whereNotNull("updated_at")
                                                                  .build());

        assertEquals("status <> ? and id not in (?, ?) and score between ? and ? and updated_at is not null",
                     fragment.sql());
        assertEquals(List.of("disabled", 1, 2, 60, 100), fragment.parameters());
    }

    private enum Status {
        ACTIVE,
        DISABLED
    }

    private record OpaqueDomainValue() {
    }

    private static final class OpaqueDriverValue {
    }

    /** 实现 Iterable 但在扩展 term 看来是一个不可拆分的驱动参数。 */
    private static final class OpaqueIterableValue implements Iterable<String> {

        @Override
        public java.util.Iterator<String> iterator() {
            return List.of("driver-internal").iterator();
        }
    }

    /** 实现 Collection 但在扩展 term 看来是不可拆分的驱动参数。 */
    private static final class OpaqueCollectionValue extends AbstractCollection<String> {

        @Override
        public java.util.Iterator<String> iterator() {
            return List.of("driver-internal").iterator();
        }

        @Override
        public int size() {
            return 1;
        }
    }
}
