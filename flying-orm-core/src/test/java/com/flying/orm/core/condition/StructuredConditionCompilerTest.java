package com.flying.orm.core.condition;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.RelationTermPackage;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证前端传来的结构化条件只能编译成安全 AST，不能把字段名、操作符或 SQL 文本混进来。
 *
 * @author wangr
 * @date 2026-07-24
 * @version v1.0
 */
class StructuredConditionCompilerTest {

    @Test
    void rejectsDepthSettingsThatCouldExhaustTheCompilerStack() {
        assertThrows(IllegalArgumentException.class,
                     () -> StructuredConditionPolicy.defaults().withMaxDepth(65));
    }

    /**
     * 前端可以传树状 and/or 条件，业务自定义操作符也能显式放行。
     */
    @Test
    void compilesFrontendConditionTreeAndKeepsSqlParameterized() {
        DynamicForm form = usersForm();
        StructuredConditionInput input = StructuredConditionInput.and(
                StructuredConditionInput.term("status", "eq", "enabled"),
                StructuredConditionInput.or(
                        StructuredConditionInput.term("name", "like", "王%"),
                        StructuredConditionInput.term("userId", "user-in-org", "org-1")));
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults()
                                                                   .allowOperator("user-in-org");

        ConditionGroup where = StructuredConditionCompiler.create().compile(form, input, policy);
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addDefaultTerms()
                                          .addTermPackage(RelationTermPackage.of("user-organization",
                                                                                "org_user",
                                                                                "ou",
                                                                                "user_id",
                                                                                "org_id",
                                                                                "user-in-org",
                                                                                "user-not-in-org"))
                                          .build();

        SqlFragment fragment = renderer.renderWhere(where);

        assertEquals("status = ? and (name like ? or exists (select 1 from org_user ou where ou.user_id = userId and ou.org_id = ?))",
                     fragment.sql());
        assertEquals(List.of("enabled", "王%", "org-1"), fragment.parameters());
        assertFalse(fragment.sql().contains("enabled"));
        assertFalse(fragment.sql().contains("org-1"));
    }

    /** 默认前端白名单必须开放忽略大小写的 LIKE，同时继续使用统一参数绑定。 */
    @Test
    void compilesCaseInsensitiveLikeTermsFromStructuredInput() {
        ConditionGroup where = StructuredConditionCompiler.create().compile(
                usersForm(),
                StructuredConditionInput.and(
                        StructuredConditionInput.term("name", "like-ignore-case", "%Alice%"),
                        StructuredConditionInput.term("name", "not-like-ignore-case", "%Admin%")),
                StructuredConditionPolicy.defaults());

        SqlFragment fragment = SqlRenderer.builder().addDefaultTerms().build().renderWhere(where);

        assertEquals("lower(name) like lower(?) and lower(name) not like lower(?)", fragment.sql());
        assertEquals(List.of("%Alice%", "%Admin%"), fragment.parameters());
    }

    /** LIKE 家族只面向文本字段；数值字段即使值能转成整数，也不能生成跨库不一致的 age like ?。 */
    @Test
    void rejectsStructuredLikeOperatorsOnNonTextFields() {
        StructuredConditionCompiler compiler = StructuredConditionCompiler.create();

        for (String operator : List.of("like", "not-like", "like-ignore-case", "not-like-ignore-case")) {
            StructuredConditionException error = assertThrows(
                    StructuredConditionException.class,
                    () -> compiler.compile(usersForm(), StructuredConditionInput.term("age", operator, "18")));

            assertEquals(StructuredConditionErrorCode.VALUE_TYPE_MISMATCH, error.code());
            assertEquals("conditions.value", error.path());
            assertEquals("age", error.field());
        }
    }

    /**
     * 前端传未知字段、未知操作符或混合节点时直接失败，不能静默忽略不安全输入。
     */
    @Test
    void rejectsUnsafeFrontendConditionShape() {
        StructuredConditionCompiler compiler = StructuredConditionCompiler.create();
        DynamicForm form = usersForm();

        assertThrows(IllegalArgumentException.class,
                     () -> compiler.compile(form,
                                            StructuredConditionInput.term("status or 1=1", "eq", "enabled")));
        assertThrows(IllegalArgumentException.class,
                     () -> compiler.compile(form,
                                            StructuredConditionInput.term("status", "sql", "status = 'enabled'")));
        assertThrows(IllegalArgumentException.class,
                     () -> compiler.compile(form,
                                            new StructuredConditionInput("status",
                                                                         "eq",
                                                                         "enabled",
                                                                         "and",
                                                                         List.of(StructuredConditionInput.term(
                                                                                 "name",
                                                                                 "like",
                                                                                 "王%")))));
    }

    /**
     * 外部反序列化得到的子节点即使为 null，也必须在编译入口返回稳定错误码和精确前端路径。
     */
    @Test
    void reportsStructuredErrorForNullChildNode() {
        StructuredConditionException error = assertThrows(
                StructuredConditionException.class,
                () -> StructuredConditionCompiler.create().compile(
                        usersForm(),
                        new StructuredConditionInput(null,
                                                     null,
                                                     null,
                                                     "and",
                                                     Arrays.asList((StructuredConditionInput) null))));

        assertEquals(StructuredConditionErrorCode.INVALID_NODE_SHAPE, error.code());
        assertEquals("conditions[0]", error.path());
    }

    /**
     * 限制树深度和集合大小，避免前端条件把编译器或数据库拖垮。
     */
    @Test
    void rejectsConditionsThatExceedPolicyLimits() {
        StructuredConditionCompiler compiler = StructuredConditionCompiler.create();
        DynamicForm form = usersForm();
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults()
                                                                   .withMaxDepth(1)
                                                                   .withMaxCollectionSize(2);

        assertThrows(IllegalArgumentException.class,
                     () -> compiler.compile(form,
                                            StructuredConditionInput.and(
                                                    StructuredConditionInput.or(
                                                            StructuredConditionInput.term("name", "like", "王%"))),
                                            policy));
        assertThrows(IllegalArgumentException.class,
                     () -> compiler.compile(form,
                                            StructuredConditionInput.term("id", "in", List.of("u1", "u2", "u3")),
                                            policy));
    }

    /**
     * 前端 JSON 里来的值常常是字符串，这里按动态字段类型提前转成更稳的参数值。
     */
    @Test
    void normalizesFrontendValuesByDynamicFieldType() {
        StructuredConditionCompiler compiler = StructuredConditionCompiler.create();
        DynamicForm form = usersForm();
        StructuredConditionInput input = StructuredConditionInput.and(
                StructuredConditionInput.term("age", "eq", "18"),
                StructuredConditionInput.term("enabled", "eq", "true"),
                StructuredConditionInput.term("birthday", "eq", "2026-07-29"),
                StructuredConditionInput.term("loginTime", "eq", "09:30:00"),
                StructuredConditionInput.term("score", "in", List.of("1", "2", "3")));

        ConditionGroup where = compiler.compile(form, input);
        List<TermCondition> terms = where.children()
                                         .stream()
                                         .map(TermCondition.class::cast)
                                         .toList();

        assertEquals(18, terms.get(0).value());
        assertEquals(true, terms.get(1).value());
        assertEquals(LocalDate.of(2026, 7, 29), terms.get(2).value());
        assertEquals(LocalTime.of(9, 30), terms.get(3).value());
        assertEquals(List.of(1, 2, 3), terms.get(4).value());
    }

    /** 方言类型必须按完整名称分类，不能因共享前缀把区间、64 位整数或带时区时间改成别的类型。 */
    @Test
    void doesNotMisclassifyDialectSpecificTypesByPrefix() {
        OffsetDateTime timestamp = OffsetDateTime.parse("2026-08-16T12:34:56+08:00");
        OffsetTime time = OffsetTime.parse("12:34:56+08:00");

        assertEquals(2_147_483_648L, compileScalar("INT8", "2147483648"));
        assertEquals(4_294_967_295L, compileScalar("INT(11) UNSIGNED", "4294967295"));
        assertEquals(4_294_967_295L, compileScalar("INT(11) ZEROFILL", "4294967295"));
        assertEquals(new BigInteger("18446744073709551615"),
                     compileScalar("BIGINT UNSIGNED", "18446744073709551615"));
        assertEquals("4294967296", compileScalar("SERIAL", "4294967296"));
        assertEquals("10101010", compileScalar("BIT(8)", "10101010"));
        assertEquals("P1D", compileScalar("INTERVAL DAY TO SECOND", "P1D"));
        assertEquals(timestamp, compileScalar("TIMESTAMP WITH TIME ZONE", timestamp));
        assertEquals(time, compileScalar("TIMETZ", time));
        assertEquals(LocalDateTime.parse("2026-08-16T12:34:56"),
                     compileScalar("TIMESTAMP(6) WITH LOCAL TIME ZONE", "2026-08-16T12:34:56"));
    }

    @Test
    void preservesLargeBinaryScalarValueInStructuredCondition() {
        byte[] payload = new byte[1_024];
        payload[0] = 7;
        DynamicForm form = DynamicForm.builder("documents", "Documents")
                                      .addField(DynamicField.of("payload", "blob"))
                                      .build();

        StructuredConditionInput input = StructuredConditionInput.term("payload", "eq", payload);
        StructuredConditionCompiler.validateStructure(input, StructuredConditionPolicy.defaults());
        ConditionGroup where = StructuredConditionCompiler.create().compile(form, input);
        payload[0] = 9;
        byte[] bound = (byte[]) ((TermCondition) where.children().getFirst()).value();

        assertEquals(1_024, bound.length);
        assertEquals(7, bound[0]);
    }

    @Test
    void preservesLargeArrayScalarValueInStructuredCondition() {
        String[] tags = new String[1_024];
        tags[0] = "before";
        DynamicForm form = DynamicForm.builder("documents", "Documents")
                                      .addField(DynamicField.of("tags", "ARRAY"))
                                      .build();

        StructuredConditionInput input = StructuredConditionInput.term("tags", "eq", tags);
        StructuredConditionCompiler.validateStructure(input, StructuredConditionPolicy.defaults());
        ConditionGroup where = StructuredConditionCompiler.create().compile(form, input);
        tags[0] = "after";
        String[] bound = (String[]) ((TermCondition) where.children().getFirst()).value();

        assertEquals(1_024, bound.length);
        assertEquals("before", bound[0]);
    }

    /** Codec 包装的 JVM fatal 必须保持原对象，不能降级为普通的结构化值类型错误。 */
    @Test
    void propagatesNestedVirtualMachineErrorFromStructuredValueCodec() {
        OutOfMemoryError fatal = new OutOfMemoryError("structured codec fatal");
        Object value = new Object();
        ValueCodec codec = new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == Object.class;
            }

            @Override
            public Object write(Object ignored) {
                throw new IllegalStateException("codec wrapper", fatal);
            }

            @Override
            public Object read(Object source, Class<?> targetType) {
                return source;
            }
        };
        DynamicForm form = DynamicForm.builder("documents", "Documents")
                                      .addField(DynamicField.of("payload", "custom_payload"))
                                      .build();

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class,
                () -> StructuredConditionCompiler.create(ValueCodecRegistry.standard().withFirst(codec))
                                                 .compile(form,
                                                          StructuredConditionInput.term("payload", "eq", value)));

        assertSame(fatal, observed);
    }

    /**
     * 业务 term 的值不一定是字段值。比如 user-in-org 挂在 userId 字段上，但传进来的是机构 ID。
     */
    @Test
    void keepsCustomOperatorValueWithoutFieldTypeConversion() {
        DynamicForm form = DynamicForm.builder("users", "Users")
                                      .addField(DynamicField.of("userId", "bigint"))
                                      .build();
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults()
                                                                   .allowOperator("user-in-org");

        ConditionGroup where = StructuredConditionCompiler.create()
                                                          .compile(form,
                                                                   StructuredConditionInput.term("userId",
                                                                                                 "user-in-org",
                                                                                                 "org-1"),
                                                                   policy);
        TermCondition term = (TermCondition) where.children().getFirst();

        assertEquals("userId", term.field());
        assertEquals("user-in-org", term.operator());
        assertEquals("org-1", term.value());
    }

    /** 标准 term 的值形状不能被业务注册表改写，否则渲染器收到的参数结构会失去保证。 */
    @Test
    void rejectsCustomRegistryThatChangesStandardTermShape() {
        assertThrows(IllegalArgumentException.class,
                     () -> TermRegistry.builder()
                                       .add(TermHandler.simple("=",
                                                               ConditionValueShape.SCALAR_OR_COLLECTION))
                                       .build());
    }

    /**
     * 字段级 operator 白名单能把同一个全局操作符限制在该用的字段上。
     */
    @Test
    void rejectsOperatorWhenFieldDoesNotAllowIt() {
        StructuredConditionCompiler compiler = StructuredConditionCompiler.create();
        DynamicForm form = usersForm();
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults()
                                                                   .allowFieldOperators("name", List.of("eq", "like"))
                                                                   .allowFieldOperators("age", List.of("eq", "gt", "lt"));

        ConditionGroup where = compiler.compile(form,
                                                StructuredConditionInput.term("name", "like", "王%"),
                                                policy);
        TermCondition term = (TermCondition) where.children().getFirst();

        assertEquals("name", term.field());
        assertEquals("like", term.operator());
        assertThrows(IllegalArgumentException.class,
                     () -> compiler.compile(form, StructuredConditionInput.term("name", "gt", "王%"), policy));
    }

    /**
     * 预设策略给上层一个开箱即用的安全档位，还能继续叠加字段限制。
     */
    @Test
    void usesPresetPolicyAndKeepsItCustomizable() {
        StructuredConditionCompiler compiler = StructuredConditionCompiler.create();
        DynamicForm form = usersForm();
        StructuredConditionPolicy policy = StructuredConditionPolicies.publicApi(List.of("name", "age"))
                                                                      .allowFieldOperators("name",
                                                                                           List.of("eq", "like"));

        ConditionGroup where = compiler.compile(form,
                                                StructuredConditionInput.term("name", "like", "王%"),
                                                policy);
        TermCondition term = (TermCondition) where.children().getFirst();

        assertEquals("name", term.field());
        assertEquals("like", term.operator());
        assertThrows(IllegalArgumentException.class,
                     () -> compiler.compile(form, StructuredConditionInput.term("status", "eq", "enabled"), policy));
        assertThrows(IllegalArgumentException.class,
                     () -> compiler.compile(form, StructuredConditionInput.term("name", "gt", "王%"), policy));
    }

    /** 明确传入空字段白名单表示一个字段都不开放，不能退化成默认的全部字段。 */
    @Test
    void emptyAllowedFieldListDeniesEveryFrontendField() {
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults().allowOnlyFields(List.of());

        StructuredConditionException error = assertThrows(
                StructuredConditionException.class,
                () -> StructuredConditionCompiler.create().compile(
                        usersForm(), StructuredConditionInput.term("name", "eq", "王"), policy));

        assertEquals(StructuredConditionErrorCode.FIELD_NOT_ALLOWED, error.code());
        assertEquals("conditions.field", error.path());
    }

    /**
     * 编译失败会带上稳定错误码和定位信息，上层不用再解析异常文本。
     */
    @Test
    void exposesStructuredErrorDetailsWhenCompilationFails() {
        StructuredConditionCompiler compiler = StructuredConditionCompiler.create();
        DynamicForm form = usersForm();
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults()
                                                                   .allowFieldOperators("name", List.of("eq", "like"));

        StructuredConditionException unknownField = assertThrows(StructuredConditionException.class,
                                                                 () -> compiler.compile(
                                                                         form,
                                                                         StructuredConditionInput.term("missing",
                                                                                                       "eq",
                                                                                                       "x")));
        assertEquals(StructuredConditionErrorCode.FIELD_NOT_ALLOWED, unknownField.code());
        assertEquals("conditions.field", unknownField.path());
        assertEquals("missing", unknownField.field());
        assertFalse(unknownField.getMessage().contains("missing"));

        StructuredConditionException fieldOperator = assertThrows(StructuredConditionException.class,
                                                                  () -> compiler.compile(
                                                                          form,
                                                                          StructuredConditionInput.term("name",
                                                                                                        "gt",
                                                                                                        "王%"),
                                                                          policy));
        assertEquals(StructuredConditionErrorCode.FIELD_OPERATOR_NOT_ALLOWED, fieldOperator.code());
        assertEquals("conditions.operator", fieldOperator.path());
        assertEquals("name", fieldOperator.field());
        assertEquals("gt", fieldOperator.operator());
        assertFalse(fieldOperator.getMessage().contains("name"));
        assertFalse(fieldOperator.getMessage().contains("gt"));

        String sensitiveLogic = "xor-password=must-not-leak";
        StructuredConditionException logic = assertThrows(
                StructuredConditionException.class,
                () -> compiler.compile(form, new StructuredConditionInput(
                        null,
                        null,
                        null,
                        sensitiveLogic,
                        List.of(StructuredConditionInput.term("name", "eq", "x")))));
        assertEquals(StructuredConditionErrorCode.LOGIC_NOT_ALLOWED, logic.code());
        assertFalse(logic.getMessage().contains(sensitiveLogic));

        StructuredConditionException conversion = assertThrows(StructuredConditionException.class,
                                                               () -> compiler.compile(
                                                                       form,
                                                                       StructuredConditionInput.term("age",
                                                                                                     "eq",
                                                                                                     "abc")));
        assertEquals(StructuredConditionErrorCode.VALUE_TYPE_MISMATCH, conversion.code());
        assertEquals("conditions.value", conversion.path());
        assertEquals("age", conversion.field());
        assertEquals(conversion.code().name(), conversion.toErrorReport().code());
        assertEquals(conversion.path(), conversion.toErrorReport().path());
    }

    /** 数值转换失败只保留稳定结构化信息，异常和完整 cause 链都不能包含前端原值。 */
    @Test
    void neverExposesRawNumericConversionValueInStructuredErrorCauseChain() {
        String secret = "secret-token";
        StructuredConditionCompiler compiler = StructuredConditionCompiler.create();
        DynamicForm form = usersForm();
        DynamicForm decimalForm = DynamicForm.builder("numbers", "Numbers")
                                             .addField(DynamicField.of("amount", "decimal(19, 2)"))
                                             .build();

        StructuredConditionException integer = assertThrows(
                StructuredConditionException.class,
                () -> compiler.compile(form, StructuredConditionInput.term("age", "eq", secret)));
        StructuredConditionException decimal = assertThrows(
                StructuredConditionException.class,
                () -> compiler.compile(decimalForm, StructuredConditionInput.term("amount", "eq", secret)));

        assertEquals(StructuredConditionErrorCode.VALUE_TYPE_MISMATCH, integer.code());
        assertEquals("age", integer.field());
        assertEquals(StructuredConditionErrorCode.VALUE_TYPE_MISMATCH, decimal.code());
        assertEquals("amount", decimal.field());
        assertNull(integer.getCause());
        assertNull(decimal.getCause());
        assertCauseChainDoesNotContain(integer, secret);
        assertCauseChainDoesNotContain(decimal, secret);
    }

    /**
     * 分组里的错误要能定位到前端数组下标，前端才能把错误标到具体条件行上。
     */
    @Test
    void reportsFrontendArrayPathForNestedConditionErrors() {
        StructuredConditionCompiler compiler = StructuredConditionCompiler.create();
        DynamicForm form = usersForm();
        StructuredConditionInput input = StructuredConditionInput.and(
                StructuredConditionInput.term("name", "like", "王%"),
                StructuredConditionInput.term("status", "eq", "enabled"),
                StructuredConditionInput.term("age", "eq", "abc"));

        StructuredConditionException error = assertThrows(StructuredConditionException.class,
                                                          () -> compiler.compile(form, input));

        assertEquals(StructuredConditionErrorCode.VALUE_TYPE_MISMATCH, error.code());
        assertEquals("conditions[2].value", error.path());
        assertEquals("age", error.field());
    }

    /**
     * 层级过深也要给到稳定路径，不然前端只能看到一条泛泛的失败提示。
     */
    @Test
    void reportsPathWhenNestedConditionExceedsDepthLimit() {
        StructuredConditionCompiler compiler = StructuredConditionCompiler.create();
        DynamicForm form = usersForm();
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults().withMaxDepth(1);

        StructuredConditionException error = assertThrows(StructuredConditionException.class,
                                                          () -> compiler.compile(
                                                                  form,
                                                                  StructuredConditionInput.and(
                                                                          StructuredConditionInput.or(
                                                                                  StructuredConditionInput.term("name",
                                                                                                                "like",
                                                                                                                "王%"))),
                                                                  policy));

        assertEquals(StructuredConditionErrorCode.DEPTH_EXCEEDED, error.code());
        assertEquals("conditions[0]", error.path());
    }

    @Test
    void honorsConfiguredStringLimitAboveTheDefault() {
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults().withMaxStringLength(5_000);
        String value = "x".repeat(4_500);

        ConditionGroup where = StructuredConditionCompiler.create().compile(
                usersForm(), StructuredConditionInput.term("name", "eq", value), policy);

        assertEquals(value, ((TermCondition) where.children().getFirst()).value());
    }

    /** 字段、操作符和逻辑同属前端结构文本，必须受同一字符串预算约束且不能回显超长原文。 */
    @Test
    void boundsStructuredNamesBeforePolicyLookupAndErrorReporting() {
        StructuredConditionCompiler compiler = StructuredConditionCompiler.create();
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults().withMaxStringLength(4);
        DynamicForm form = usersForm();

        StructuredConditionException field = assertThrows(StructuredConditionException.class,
                () -> compiler.compile(form, StructuredConditionInput.term("secret", "eq", "x"), policy));
        assertEquals(StructuredConditionErrorCode.FIELD_NOT_ALLOWED, field.code());
        assertEquals("conditions.field", field.path());
        assertNull(field.toErrorReport().field());

        StructuredConditionException operator = assertThrows(StructuredConditionException.class,
                () -> compiler.compile(form, StructuredConditionInput.term("name", "equals", "x"), policy));
        assertEquals(StructuredConditionErrorCode.OPERATOR_NOT_ALLOWED, operator.code());
        assertEquals("conditions.operator", operator.path());
        assertNull(operator.toErrorReport().field());

        StructuredConditionInput group = new StructuredConditionInput(
                null, null, null, "and-secret", List.of(StructuredConditionInput.term("name", "eq", "x")));
        StructuredConditionException logic = assertThrows(StructuredConditionException.class,
                () -> compiler.compile(form, group, policy));
        assertEquals(StructuredConditionErrorCode.LOGIC_NOT_ALLOWED, logic.code());
        assertEquals("conditions.logic", logic.path());
        assertNull(logic.toErrorReport().field());
    }

    @Test
    void cleansCollectionElementsAndSupportsExplicitNullTerms() {
        StructuredConditionInput input = StructuredConditionInput.and(
                StructuredConditionInput.term("name", "eq", "  张三  "),
                StructuredConditionInput.term("score", "in", Arrays.asList(" ", null, "1", "2")),
                StructuredConditionInput.term("status", "is-null", null));

        ConditionGroup where = StructuredConditionCompiler.create().compile(usersForm(), input);
        List<TermCondition> terms = where.children().stream().map(TermCondition.class::cast).toList();

        assertEquals("张三", terms.get(0).value());
        assertEquals(List.of(1, 2), terms.get(1).value());
        assertEquals("is-null", terms.get(2).operator());
        assertNull(terms.get(2).value());
    }

    @Test
    void reportsStableRangeAndNoneValueErrors() {
        StructuredConditionCompiler compiler = StructuredConditionCompiler.create();
        DynamicForm form = usersForm();

        StructuredConditionException size = assertThrows(
                StructuredConditionException.class,
                () -> compiler.compile(form,
                                       StructuredConditionInput.and(
                                               StructuredConditionInput.term("age", "between", List.of("18")))));
        assertEquals(StructuredConditionErrorCode.VALUE_RANGE_SIZE_INVALID, size.code());
        assertEquals("conditions[0].value", size.path());

        StructuredConditionException order = assertThrows(
                StructuredConditionException.class,
                () -> compiler.compile(form,
                                       StructuredConditionInput.and(
                                               StructuredConditionInput.term("age",
                                                                             "between",
                                                                             List.of("20", "18")))));
        assertEquals(StructuredConditionErrorCode.VALUE_RANGE_ORDER_INVALID, order.code());
        assertEquals("conditions[0].value", order.path());

        StructuredConditionException noneValue = assertThrows(
                StructuredConditionException.class,
                () -> compiler.compile(form,
                                       StructuredConditionInput.term("status", "is-null", "unexpected")));
        assertEquals(StructuredConditionErrorCode.VALUE_SHAPE_NOT_ALLOWED, noneValue.code());
        assertEquals("conditions.value", noneValue.path());
    }

    @Test
    void rejectsNestedAndCyclicCollectionValuesWithoutRecursingIntoThem() {
        StructuredConditionCompiler compiler = StructuredConditionCompiler.create();
        DynamicForm form = usersForm();

        StructuredConditionException nested = assertThrows(
                StructuredConditionException.class,
                () -> compiler.compile(form,
                                       StructuredConditionInput.term("score", "in", List.of(List.of("1")))));
        assertEquals(StructuredConditionErrorCode.VALUE_SHAPE_NOT_ALLOWED, nested.code());
        assertEquals("conditions.value[0]", nested.path());

        List<Object> cyclic = new ArrayList<>();
        cyclic.add(cyclic);
        StructuredConditionException cycle = assertThrows(
                StructuredConditionException.class,
                () -> compiler.compile(form, StructuredConditionInput.term("score", "in", cyclic)));
        assertEquals(StructuredConditionErrorCode.VALUE_SHAPE_NOT_ALLOWED, cycle.code());
        assertEquals("conditions.value[0]", cycle.path());
    }

    @Test
    void snapshotsOneShotIterableBeforeValidationAndNormalization() {
        Iterable<String> oneShot = new Iterable<>() {
            private final Iterator<String> values = List.of("1", "2").iterator();

            @Override
            public Iterator<String> iterator() {
                return values;
            }
        };

        ConditionGroup where = StructuredConditionCompiler.create().compile(
                usersForm(), StructuredConditionInput.term("score", "in", oneShot));
        SqlFragment fragment = SqlRenderer.builder().addDefaultTerms().build().renderWhere(where);

        assertEquals("score in (?, ?)", fragment.sql());
        assertEquals(List.of(1, 2), fragment.parameters());
    }

    /** 恶意 Iterable 的异常文本不能绕过稳定错误模型泄露到 cause 链。 */
    @Test
    void sanitizesFailureRaisedWhileSnapshottingIterable() {
        String secret = "secret-token";
        Iterable<String> values = () -> {
            throw new IllegalStateException(secret);
        };

        StructuredConditionException error = assertThrows(
                StructuredConditionException.class,
                () -> StructuredConditionCompiler.create().compile(
                        usersForm(), StructuredConditionInput.term("score", "in", values)));

        assertEquals(StructuredConditionErrorCode.VALUE_TYPE_MISMATCH, error.code());
        assertEquals("conditions.value", error.path());
        assertEquals("score", error.field());
        assertNull(error.getCause());
        assertEquals(0, error.getSuppressed().length);
        assertCauseChainDoesNotContain(error, secret);
    }

    /** 原始容器预检也属于前端错误边界，不能把自定义容器的异常文本或 cause 暴露给调用方。 */
    @Test
    void sanitizesFailureRaisedDuringRawCollectionValidation() {
        String secret = "secret-token";
        AbstractCollection<Object> values = new AbstractCollection<>() {
            @Override
            public Iterator<Object> iterator() {
                throw new IllegalStateException(secret);
            }

            @Override
            public int size() {
                return 1;
            }
        };

        StructuredConditionException error = assertThrows(
                StructuredConditionException.class,
                () -> StructuredConditionCompiler.validateStructure(
                        StructuredConditionInput.term("score", "in", values),
                        StructuredConditionPolicy.defaults()));

        assertEquals(StructuredConditionErrorCode.VALUE_SHAPE_NOT_ALLOWED, error.code());
        assertEquals("conditions.value", error.path());
        assertNull(error.getCause());
        assertEquals(0, error.getSuppressed().length);
        assertCauseChainDoesNotContain(error, secret);
    }

    /** 原始容器回调包装的 JVM fatal 不能被稳定错误替换，必须保持同一对象出站。 */
    @Test
    void propagatesFatalRaisedDuringRawCollectionValidation() {
        OutOfMemoryError fatal = new OutOfMemoryError("fatal");
        AbstractCollection<Object> values = new AbstractCollection<>() {
            @Override
            public Iterator<Object> iterator() {
                throw new IllegalStateException("wrapper", fatal);
            }

            @Override
            public int size() {
                return 1;
            }
        };

        OutOfMemoryError actual = assertThrows(
                OutOfMemoryError.class,
                () -> StructuredConditionCompiler.validateStructure(
                        StructuredConditionInput.term("score", "in", values),
                        StructuredConditionPolicy.defaults()));

        assertSame(fatal, actual);
    }

    /** 超限 Iterable 只读取到确认越界的那个元素，不能先把整个输入复制进内存。 */
    @Test
    void stopsReadingIterableAsSoonAsCollectionLimitIsExceeded() {
        AtomicInteger reads = new AtomicInteger();
        Iterable<String> values = () -> new Iterator<>() {
            private final Iterator<String> delegate = List.of("1", "2", "3", "4").iterator();

            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public String next() {
                reads.incrementAndGet();
                return delegate.next();
            }
        };

        StructuredConditionException error = assertThrows(
                StructuredConditionException.class,
                () -> StructuredConditionCompiler.create().compile(
                        usersForm(),
                        StructuredConditionInput.term("score", "in", values),
                        StructuredConditionPolicy.defaults().withMaxCollectionSize(2)));

        assertEquals(StructuredConditionErrorCode.VALUE_COLLECTION_TOO_LARGE, error.code());
        assertEquals(3, reads.get());
    }

    /** 扩展适配器包装 JSON、数组或向量之前，原始值图仍必须服从深度、节点和字符串预算。 */
    @Test
    void validatesExtensionValueGraphBeforeAdaptation() {
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults()
                                                                    .withMaxDepth(2)
                                                                    .withMaxNodes(2)
                                                                    .withMaxStringLength(4);
        StructuredConditionInput depth = StructuredConditionInput.term(
                "payload", "custom", Map.of("a", Map.of("b", Map.of("c", 1))));
        StructuredConditionInput nodes = StructuredConditionInput.term(
                "payload", "custom", Map.of("a", Map.of(), "b", Map.of()));
        StructuredConditionInput text = StructuredConditionInput.term(
                "payload", "custom", Map.of("v", "12345"));

        StructuredConditionException depthError = assertThrows(
                StructuredConditionException.class,
                () -> StructuredConditionCompiler.validateStructure(depth, policy));
        StructuredConditionException nodesError = assertThrows(
                StructuredConditionException.class,
                () -> StructuredConditionCompiler.validateStructure(nodes, policy));
        StructuredConditionException textError = assertThrows(
                StructuredConditionException.class,
                () -> StructuredConditionCompiler.validateStructure(text, policy));

        assertEquals(StructuredConditionErrorCode.DEPTH_EXCEEDED, depthError.code());
        assertEquals(StructuredConditionErrorCode.NODE_COUNT_EXCEEDED, nodesError.code());
        assertEquals(StructuredConditionErrorCode.VALUE_TOO_LONG, textError.code());
    }

    /** 可配置预算不能高于后续 AST 与渲染硬边界，避免编译成功后才以普通异常失败。 */
    @Test
    void rejectsConfiguredBudgetsBeyondExecutionHardLimits() {
        assertThrows(IllegalArgumentException.class,
                     () -> StructuredConditionPolicy.defaults().withMaxNodes(10_001));
        assertThrows(IllegalArgumentException.class,
                     () -> StructuredConditionPolicy.defaults().withMaxCollectionSize(1_001));
    }

    private static DynamicForm usersForm() {
        return DynamicForm.builder("users", "Users")
                          .addField(DynamicField.primaryKey("id", "varchar"))
                          .addField(DynamicField.of("name", "varchar"))
                          .addField(DynamicField.of("status", "varchar"))
                          .addField(DynamicField.of("userId", "varchar"))
                          .addField(DynamicField.of("age", "integer"))
                          .addField(DynamicField.of("enabled", "boolean"))
                          .addField(DynamicField.of("birthday", "date"))
                          .addField(DynamicField.of("loginTime", "time"))
                          .addField(DynamicField.of("score", "integer"))
                          .build();
    }

    private static void assertCauseChainDoesNotContain(Throwable error, String forbidden) {
        for (Throwable current = error; current != null; current = current.getCause()) {
            assertFalse(current.toString().contains(forbidden));
        }
    }

    private static Object compileScalar(String dataType, Object value) {
        DynamicForm form = DynamicForm.builder("typed_values", "Typed values")
                                      .addField(DynamicField.of("value", dataType))
                                      .build();
        ConditionGroup where = StructuredConditionCompiler.create().compile(
                form, StructuredConditionInput.term("value", "eq", value));
        return ((TermCondition) where.children().getFirst()).value();
    }
}
