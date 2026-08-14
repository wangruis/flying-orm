package com.flying.orm.core.param;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.condition.LogicalOperator;
import com.flying.orm.core.condition.TermHandler;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.condition.TermRegistry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证请求参数可以按 Java 规则编译为动态条件树。
 *
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
class ParameterConditionCompilerTest {

    /**
     * 验证参数名可以映射到字段、term id 和业务值，支持业务自定义 term。
     */
    @Test
    void compilesRequestParametersIntoConditionTree() {
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                                                                        .add(ParameterConditionSpec.of("status",
                                                                                                      "status",
                                                                                                      "="))
                                                                        .add(ParameterConditionSpec.of("orgId",
                                                                                                      "userId",
                                                                                                      "user-in-org"))
                                                                        .build();

        ConditionGroup group = compiler.compile(Map.of("status", "enabled", "orgId", "org-1"));

        assertEquals(2, group.children().size());
        TermCondition status = assertInstanceOf(TermCondition.class, group.children().get(0));
        TermCondition org = assertInstanceOf(TermCondition.class, group.children().get(1));
        assertEquals("status", status.field());
        assertEquals("=", status.operator());
        assertEquals("enabled", status.value());
        assertEquals("userId", org.field());
        assertEquals("user-in-org", org.operator());
        assertEquals("org-1", org.value());
    }

    /**
     * 验证缺失参数、null 和空白字符串不会生成条件。
     */
    @Test
    void skipsMissingNullAndBlankParameterValues() {
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                                                                        .add(ParameterConditionSpec.of("status",
                                                                                                      "status",
                                                                                                      "="))
                                                                        .add(ParameterConditionSpec.of("name",
                                                                                                      "name",
                                                                                                      "like"))
                                                                        .add(ParameterConditionSpec.of("ids",
                                                                                                      "id",
                                                                                                      "in"))
                                                                        .build();

        ConditionGroup group = compiler.compile(Map.of("name", " ", "ids", List.of()));

        assertEquals(0, group.children().size());
    }

    /** 判空、转换和建 AST 必须复用同一份快照，单次 Iterable 不能在判空阶段被提前耗尽。 */
    @Test
    void compilesOneShotCollectionWithoutConsumingItDuringEmptyChecks() {
        Iterable<String> oneShot = new Iterable<>() {
            private final Iterator<String> values = List.of("u-1", "u-2").iterator();

            @Override
            public Iterator<String> iterator() {
                return values;
            }
        };
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                                                                        .add(ParameterConditionSpec.of(
                                                                                "ids", "id", "in"))
                                                                        .build();

        ConditionGroup group = compiler.compile(Map.of("ids", oneShot));

        assertEquals(List.of("u-1", "u-2"),
                     assertInstanceOf(TermCondition.class, group.children().getFirst()).value());
    }

    @Test
    void preservesLargeArrayAsScalarParameterConditionValue() {
        String[] tags = new String[1_024];
        tags[0] = "before";
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                                                                        .add(ParameterConditionSpec.of(
                                                                                "tags", "tags", "="))
                                                                        .build();

        ConditionGroup group = compiler.compile(Map.of("tags", tags));
        tags[0] = "after";
        String[] bound = (String[]) assertInstanceOf(TermCondition.class,
                                                       group.children().getFirst()).value();

        assertEquals(1_024, bound.length);
        assertEquals("before", bound[0]);
    }

    /** 验证构建后修改标量数组默认值不会改变编译器保存的默认条件。 */
    @Test
    void snapshotsScalarArrayDefaultValueWhenCompilerIsBuilt() {
        String[] tags = {"before"};
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                                                                        .add(ParameterConditionSpec.builder(
                                                                                "tags", "tags", "=")
                                                                                                   .defaultValue(tags)
                                                                                                   .build())
                                                                        .build();

        tags[0] = "after";
        ConditionGroup group = compiler.compile(Map.of());

        String[] bound = (String[]) assertInstanceOf(TermCondition.class,
                                                       group.children().getFirst()).value();
        assertEquals("before", bound[0]);
    }

    /** 已发布规则的数组访问器不能反向改写 compiler 后续编译使用的默认快照。 */
    @Test
    void keepsScalarArrayDefaultImmutableWhenExposedSpecsAreRead() {
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                                                                        .add(ParameterConditionSpec.builder(
                                                                                "tags", "tags", "=")
                                                                                                   .defaultValue(new String[]{"before"})
                                                                                                   .build())
                                                                        .build();

        String[] exposedDefault = (String[]) compiler.specs().getFirst().defaultValue();
        exposedDefault[0] = "after";
        ConditionGroup group = compiler.compile(Map.of());

        String[] bound = (String[]) assertInstanceOf(TermCondition.class,
                                                       group.children().getFirst()).value();
        assertEquals("before", bound[0]);
    }

    /** 数组默认值的嵌套、共享和自环结构在构造及访问边界都必须保持隔离。 */
    @Test
    void snapshotsNestedArrayDefaultGraphAtBothBoundaries() {
        Object marker = new Object();
        byte[] shared = {1, 2};
        Object[] cycle = new Object[1];
        cycle[0] = cycle;
        Object[] source = {shared, shared, cycle, marker};
        ParameterConditionSpec spec = ParameterConditionSpec.builder("key", "key", "=")
                                                              .defaultValue(source)
                                                              .build();

        shared[0] = 9;
        cycle[0] = new Object();
        source[3] = new Object();
        Object[] first = (Object[]) spec.defaultValue();

        assertNotSame(source, first);
        assertSame(first[0], first[1]);
        assertArrayEquals(new byte[]{1, 2}, (byte[]) first[0]);
        assertSame(first[2], ((Object[]) first[2])[0]);
        assertSame(marker, first[3]);

        ((byte[]) first[0])[0] = 8;
        ((Object[]) first[2])[0] = null;
        first[3] = null;
        Object[] second = (Object[]) spec.defaultValue();

        assertNotSame(first, second);
        assertNotSame(first[0], second[0]);
        assertSame(second[0], second[1]);
        assertArrayEquals(new byte[]{1, 2}, (byte[]) second[0]);
        assertSame(second[2], ((Object[]) second[2])[0]);
        assertSame(marker, second[3]);
    }

    @Test
    void rejectsStringLongerThanConfiguredLimit() {
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                                                                        .maxStringLength(4)
                                                                        .add(ParameterConditionSpec.of(
                                                                                "name", "name", "like"))
                                                                        .build();

        assertThrows(IllegalArgumentException.class,
                     () -> compiler.compile(Map.of("name", "12345")));
    }

    @Test
    void stopsReadingIterableAsSoonAsCollectionLimitIsExceeded() {
        AtomicInteger nextCalls = new AtomicInteger();
        Iterable<Integer> unbounded = () -> new Iterator<>() {
            private int value;

            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public Integer next() {
                nextCalls.incrementAndGet();
                return value++;
            }
        };
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                                                                        .maxCollectionSize(2)
                                                                        .add(ParameterConditionSpec.of(
                                                                                "ids", "id", "in"))
                                                                        .build();

        assertThrows(IllegalArgumentException.class,
                     () -> compiler.compile(Map.of("ids", unbounded)));
        assertEquals(3, nextCalls.get());
    }

    @Test
    void stripsStringsCleansCollectionsAndUsesRegisteredBusinessShape() {
        TermRegistry terms = TermRegistry.builder()
                                         .add(TermHandler.simple("users-in-orgs", ConditionValueShape.COLLECTION))
                                         .build();
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                                                                        .terms(terms)
                                                                        .add(ParameterConditionSpec.of("name",
                                                                                                      "name",
                                                                                                      "like"))
                                                                        .add(ParameterConditionSpec.of("orgIds",
                                                                                                      "userId",
                                                                                                      "users-in-orgs"))
                                                                        .add(ParameterConditionSpec.of("status",
                                                                                                      "status",
                                                                                                      "="))
                                                                        .build();
        Map<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("name", "  张三  ");
        parameters.put("orgIds", Arrays.asList(" ", null, " org-1 ", "org-2"));
        parameters.put("status", "   ");

        ConditionGroup group = compiler.compile(parameters);

        assertEquals(2, group.children().size());
        assertEquals("张三", ((TermCondition) group.children().get(0)).value());
        assertEquals(List.of("org-1", "org-2"), ((TermCondition) group.children().get(1)).value());
    }

    /** 条件包和单独 term 配置是追加关系，调用先后不能改变参数编译结果。 */
    @Test
    void keepsPackageTermsWhenMoreTermsAreAddedLater() {
        TermRegistry packageTerms = TermRegistry.builder()
                                                .add(TermHandler.simple("users-in-orgs",
                                                                        ConditionValueShape.COLLECTION))
                                                .build();
        ParameterConditionPackage conditionPackage = ParameterConditionPackage.of(
                "organization-filter",
                packageTerms,
                ParameterConditionSpec.of("orgIds", "userId", "users-in-orgs"));
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                                                                        .addPackage(conditionPackage)
                                                                        .terms(TermRegistry.builder()
                                                                                           .add(TermHandler.simple(
                                                                                                   "owned-by"))
                                                                                           .build())
                                                                        .build();

        ConditionGroup group = compiler.compile(Map.of("orgIds", List.of("org-1", "org-2")));

        assertEquals(List.of("org-1", "org-2"),
                     assertInstanceOf(TermCondition.class, group.children().getFirst()).value());
    }

    /**
     * 验证重复参数规则会被拒绝，避免同一个输入参数产生多份不确定条件。
     */
    @Test
    void rejectsDuplicateParameterNames() {
        ParameterConditionCompiler.Builder builder = ParameterConditionCompiler.builder()
                                                                               .add(ParameterConditionSpec.of("status",
                                                                                                             "status",
                                                                                                             "="))
                                                                               .add(ParameterConditionSpec.of("STATUS",
                                                                                                             "enabled",
                                                                                                             "="));

        assertThrows(IllegalArgumentException.class, builder::build);
    }

    /**
     * 验证输入参数中规范化后重复的键会被拒绝，避免大小写或空白差异导致条件值不确定。
     */
    @Test
    void rejectsDuplicateNormalizedInputParameterNames() {
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                                                                        .add(ParameterConditionSpec.of("status",
                                                                                                      "status",
                                                                                                      "="))
                                                                        .build();
        String secret = "SECRET_TENANT_TOKEN";
        Map<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("secret_tenant_token", "enabled");
        parameters.put(" " + secret + " ", "disabled");

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> compiler.compile(parameters));

        assertFalse(error.getMessage().contains(secret));
    }

    /**
     * 验证默认值和转换器会在生成条件前生效，避免执行层再做参数语义处理。
     */
    @Test
    void appliesDefaultValueAndConverterBeforeCreatingCondition() {
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                                                                        .add(ParameterConditionSpec.builder("minAge",
                                                                                                           "age",
                                                                                                           ">")
                                                                                                   .defaultValue("18")
                                                                                                   .convert(value -> Integer.parseInt(value.toString()))
                                                                                                   .build())
                                                                        .build();

        ConditionGroup group = compiler.compile(Map.of());

        TermCondition age = assertInstanceOf(TermCondition.class, group.children().get(0));
        assertEquals("age", age.field());
        assertEquals(">", age.operator());
        assertEquals(18, age.value());
    }

    /** 无值 term 的显式 null 默认值表示应生成不绑定参数的条件，构建期快照不能把它当作数组处理。 */
    @Test
    void appliesExplicitNullDefaultToNoValueTerm() {
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                                                                        .add(ParameterConditionSpec.builder(
                                                                                "deleted", "deleted_at", "is-null")
                                                                                                   .defaultValue(null)
                                                                                                   .build())
                                                                        .build();

        TermCondition condition = assertInstanceOf(TermCondition.class,
                                                    compiler.compile(Map.of()).children().getFirst());

        assertEquals("deleted_at", condition.field());
        assertEquals("is-null", condition.operator());
        assertEquals(null, condition.value());
    }

    /** 编译器构造后必须与调用方持有的可变默认集合和数组隔离。 */
    @Test
    void snapshotsMutableDefaultValuesWhenCompilerIsBuilt() {
        List<String> ids = new ArrayList<>(List.of("a"));
        String[] codes = {"x"};
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                .add(ParameterConditionSpec.builder("ids", "id", "in").defaultValue(ids).build())
                .add(ParameterConditionSpec.builder("codes", "code", "in").defaultValue(codes).build())
                .build();

        ids.add("b");
        codes[0] = "changed";
        ConditionGroup group = compiler.compile(Map.of());

        TermCondition id = assertInstanceOf(TermCondition.class, group.children().get(0));
        TermCondition code = assertInstanceOf(TermCondition.class, group.children().get(1));
        assertEquals(List.of("a"), id.value());
        assertEquals(List.of("x"), code.value());
    }

    /**
     * 验证同一个参数可以通过 OR 组映射到多个字段，形成嵌套条件组。
     */
    @Test
    void compilesParameterSpecsInsideNestedOrGroup() {
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                                                                        .add(ParameterConditionSpec.of("status",
                                                                                                      "status",
                                                                                                      "="))
                                                                        .addOrGroup(ParameterConditionSpec.of("keyword",
                                                                                                             "name",
                                                                                                             "like"),
                                                                                    ParameterConditionSpec.of("keyword",
                                                                                                             "mobile",
                                                                                                             "like"))
                                                                        .build();

        ConditionGroup group = compiler.compile(Map.of("status", "enabled", "keyword", "王%"));

        assertEquals(LogicalOperator.AND, group.operator());
        assertEquals(2, group.children().size());
        ConditionGroup keyword = assertInstanceOf(ConditionGroup.class, group.children().get(1));
        assertEquals(LogicalOperator.OR, keyword.operator());
        assertEquals(2, keyword.children().size());
        assertEquals("name", assertInstanceOf(TermCondition.class, keyword.children().get(0)).field());
        assertEquals("mobile", assertInstanceOf(TermCondition.class, keyword.children().get(1)).field());
    }

    /** 参数条件的可配置集合预算不能越过下游 AST 与 SQL 渲染的统一硬边界。 */
    @Test
    void rejectsCollectionBudgetBeyondTheExecutionHardLimit() {
        assertThrows(IllegalArgumentException.class,
                     () -> ParameterConditionCompiler.builder().maxCollectionSize(1_001));
    }

    /** 重复公开配置参数不能把调用方提供的任意文本拼进异常消息。 */
    @Test
    void duplicateSpecsDoNotExposeConfiguredParameterName() {
        String secret = "tenant-secret-parameter";

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> ParameterConditionCompiler.builder()
                                                  .add(ParameterConditionSpec.of(secret, "name", "="))
                                                  .add(ParameterConditionSpec.of(secret, "email", "="))
                                                  .build());

        assertEquals("duplicate parameter condition spec", error.getMessage());
        assertFalse(error.getMessage().contains(secret));
    }
}
