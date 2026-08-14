package com.flying.orm.core.join;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证轻量 JOIN AST 在进入 SQL 渲染前已经固定数据源、字段归属、ON 和投影边界。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
class JoinQuerySpecTest {

    /** 复合 ON、条件、Scope 和投影必须形成不可变、有序的查询快照。 */
    @Test
    void buildsImmutableJoinQueryWithCompoundOnAndExplicitProjection() {
        DynamicForm users = form("users-form", "users", "id", "tenant_id", "name");
        DynamicForm orders = form("orders-form", "orders", "id", "tenant_id", "user_id", "order_no");
        ConditionGroup userWhere = ConditionGroup.and().where("name", "=", "alice").build();

        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(users);
        JoinSource root = builder.root();
        JoinSource joined = builder.join(JoinType.LEFT, orders, root, "id", "user_id");
        builder.andOn(joined, root, "tenant_id", "tenant_id")
               .where(root, userWhere)
               .scope(root, DataScope.none())
               .select(root, "name")
               .selectAs(joined, "order_no", "orderNo");

        JoinQuerySpec spec = builder.build();

        assertEquals(root, spec.root());
        assertEquals(List.of(root, joined), spec.sources());
        assertEquals(1, spec.joins().size());
        assertEquals(2, spec.joins().getFirst().on().size());
        assertEquals(List.of("s0_name", "orderNo"),
                     spec.projections().stream().map(JoinProjection::alias).toList());
        assertEquals(userWhere, spec.where(root));
        assertEquals(DataScope.none(), spec.scope(root));
        assertThrows(UnsupportedOperationException.class,
                     () -> spec.projections().add(new JoinProjection(new JoinFieldRef(root, "id"), "id")));
    }

    /** 同一物理源、自连接和没有显式投影都必须在 SQL 生成前被拒绝。 */
    @Test
    void rejectsDuplicateSourceSelfJoinAndMissingProjection() {
        DynamicForm users = form("users-form", "users", "id", "manager_id");
        JoinQuerySpec.Builder missingProjection = JoinQuerySpec.builder(users);
        JoinSource root = missingProjection.root();

        assertThrows(IllegalArgumentException.class,
                     () -> missingProjection.join(JoinType.INNER, users, root, "id", "manager_id"));
        assertThrows(IllegalStateException.class, missingProjection::build);
    }

    /** 失败的 JOIN 参数校验不能提前污染可继续使用的构建器状态。 */
    @Test
    void failedJoinDoesNotCorruptTheBuilder() {
        DynamicForm users = form("users-form", "users", "id");
        DynamicForm orders = form("orders-form", "orders", "id", "user_id");
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(users);
        JoinSource root = builder.root();

        assertThrows(NullPointerException.class,
                     () -> builder.join(null, orders, root, "id", "user_id"));

        JoinSource joined = builder.join(JoinType.INNER, orders, root, "id", "user_id");
        builder.select(root, "id").select(joined, "id");
        JoinQuerySpec spec = builder.build();

        assertEquals(List.of(root, joined), spec.sources());
        assertEquals(1, spec.joins().size());
    }

    /** 字段必须属于已经加入的源，投影结果别名在规范化后必须保持唯一。 */
    @Test
    void rejectsUnknownFieldsUnjoinedSourcesAndNormalizedAliasConflicts() {
        DynamicForm users = form("users-form", "users", "id", "name");
        DynamicForm orders = form("orders-form", "orders", "id", "user_id");
        DynamicForm archived = form("archive-form", "archived_orders", "id", "user_id");
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(users);
        JoinSource root = builder.root();
        JoinSource joined = builder.join(JoinType.RIGHT, orders, root, "id", "user_id");
        JoinSource unjoined = new JoinSource(9, archived);

        assertThrows(IllegalArgumentException.class, () -> builder.select(root, "missing"));
        assertThrows(IllegalArgumentException.class, () -> builder.select(unjoined, "id"));

        builder.selectAs(root, "name", "display_name");
        IllegalArgumentException conflict = assertThrows(
                IllegalArgumentException.class,
                () -> builder.selectAs(joined, "id", " DISPLAY_NAME "));
        assertEquals("join projection alias must be unique", conflict.getMessage());
        assertTrue(conflict.getMessage().length() < 80);
    }

    /** JOIN 查询可以按本次请求统一覆盖声明式脱敏，且默认仍遵守字段声明。 */
    @Test
    void keepsExplicitSensitiveDisplayModeInTheImmutableJoinSpec() {
        DynamicForm users = form("users-form", "users", "id", "name");
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(users);
        builder.select(builder.root(), "name");

        assertEquals(SensitiveDisplayMode.DECLARED, builder.build().sensitiveDisplayMode());
        assertEquals(SensitiveDisplayMode.MASKED, builder.masked().build().sensitiveDisplayMode());
        assertEquals(SensitiveDisplayMode.FULL, builder.showSensitive().build().sensitiveDisplayMode());
        assertEquals(SensitiveDisplayMode.DECLARED, builder.declaredDisplay().build().sensitiveDisplayMode());
    }

    /** 自动别名必须回退到 Oracle 12c 可接受的短名称，显式超长别名应在 SQL 前拒绝。 */
    @Test
    void keepsJoinProjectionAliasesWithinThePortableIdentifierLimit() {
        String longField = "a".repeat(40);
        DynamicForm users = form("users-form", "users", longField);
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(users);

        builder.select(builder.root(), longField);

        assertEquals("s0_f0", builder.build().projections().getFirst().alias());
        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> new JoinProjection(new JoinFieldRef(builder.root(), longField), "x".repeat(31)));
        assertEquals("join projection alias exceeds the portable identifier limit", failure.getMessage());
    }

    private static DynamicForm form(String id, String table, String... fields) {
        DynamicForm.Builder builder = DynamicForm.builder(id, table);
        for (String field : fields) {
            builder.addField(DynamicField.of(field, "VARCHAR"));
        }
        return builder.build();
    }
}
