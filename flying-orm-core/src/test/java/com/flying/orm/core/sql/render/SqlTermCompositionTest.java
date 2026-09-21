package com.flying.orm.core.sql.render;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.condition.TermExtensionDescriptor;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SqlTermCompositionTest {

    @TestFactory
    Stream<DynamicTest> treatsEveryPublicExtensionFormAsOneBooleanOperand() {
        return Stream.of("factory", "descriptor", "implementation").map(kind ->
                DynamicTest.dynamicTest(kind, () -> {
                    AtomicInteger calls = new AtomicInteger();
                    SqlRenderer standard = SqlRenderer.builder().addDefaultTerms().build();
                    SqlTermRenderer extension = (term, context) -> {
                        calls.incrementAndGet();
                        return standard.withIdentifierRenderer(context::identifier).renderWhere(
                                ConditionGroup.or().where(term.field(), "=", term.value())
                                        .where(term.field(), "is-null", null).build());
                    };
                    SqlTermHandler handler = switch (kind) {
                        case "factory" -> SqlTermHandler.of("eq-or-null", extension);
                        case "descriptor" -> SqlTermHandler.of(TermExtensionDescriptor.filter(
                                "eq-or-null", Set.of(), 1, 1), ConditionValueShape.SCALAR, extension);
                        default -> new SqlTermHandler() {
                            public String id() { return "eq-or-null"; }
                            public SqlFragment render(TermCondition term, SqlRenderContext context) {
                                return extension.render(term, context);
                            }
                        };
                    };
                    SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().addTerm(handler).build();
                    ConditionGroup where = renderer.conditions().where("x", "eq-or-null", 10)
                            .where("tenant_id", "=", "A").build();

                    SqlFragment result = renderer.renderWhere(where);

                    assertEquals("(x = ? or x is null) and tenant_id = ?", result.sql());
                    assertEquals(List.of(10, "A"), result.parameters());
                    assertEquals(1, calls.get());
                }));
    }
}
