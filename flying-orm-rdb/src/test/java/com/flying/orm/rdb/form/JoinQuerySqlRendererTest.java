package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.RelationTermPackage;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.time.OffsetTime;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证四库共享的 JOIN 渲染器只输出受控标识符，并保持外连接保护条件与参数顺序。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
class JoinQuerySqlRendererTest {

    /** LEFT JOIN 可选右侧的行级保护必须在连接前过滤，不能落入最终 WHERE。 */
    @Test
    void rendersLeftOuterJoinWithProtectedOptionalSourceAndBusinessWhere() {
        DynamicForm users = form("users", "id", "name");
        DynamicForm orders = form("orders", "id", "user_id", "order_no", "deleted");
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(users);
        JoinSource root = builder.root();
        JoinSource joined = builder.join(JoinType.LEFT, orders, root, "id", "user_id");
        JoinQuerySpec spec = builder.selectAs(root, "name", "userName")
                                    .selectAs(joined, "order_no", "orderNo")
                                    .where(root, ConditionGroup.and().where("name", "=", "alice").build())
                                    .build();

        SqlRequest request = renderer().joinQueries().select(
                spec,
                Map.of(joined, ConditionGroup.and().where("deleted", "=", 0).build()));

        assertEquals("select t0.name as userName, t1.order_no as orderNo "
                             + "from users t0 left outer join "
                             + "(select * from orders where deleted = ?) t1 "
                             + "on t0.id = t1.user_id where t0.name = ?",
                     request.sql());
        assertEquals(java.util.List.of(0, "alice"), request.parameters());
    }

    /** RIGHT JOIN 左侧保护通过受控派生关系保留 null-extension 语义。 */
    @Test
    void rendersRightOuterJoinWithoutMovingLeftProtectionToFinalWhere() {
        DynamicForm users = form("users", "id", "tenant_id", "name");
        DynamicForm orders = form("orders", "id", "tenant_id", "user_id");
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(users);
        JoinSource root = builder.root();
        JoinSource joined = builder.join(JoinType.RIGHT, orders, root, "id", "user_id");
        builder.andOn(joined, root, "tenant_id", "tenant_id");
        JoinQuerySpec spec = builder.select(root, "name").select(joined, "id").build();

        SqlRequest request = renderer().joinQueries().select(
                spec,
                Map.of(root, ConditionGroup.and().where("tenant_id", "=", "tenant-a").build()));

        assertEquals("select t0.name as s0_name, t1.id as s1_id "
                             + "from (select * from users where tenant_id = ?) t0 "
                             + "right outer join orders t1 on t0.id = t1.user_id "
                             + "and t0.tenant_id = t1.tenant_id",
                     request.sql());
        assertEquals(java.util.List.of("tenant-a"), request.parameters());
    }

    /** 分页与 count 必须复用同一 JOIN、Scope 和业务条件，只替换投影并追加方言分页。 */
    @Test
    void rendersJoinPageAndCountFromTheSameProtectedPlan() {
        DynamicForm users = form("users", "id", "tenant_id", "name");
        DynamicForm orders = form("orders", "id", "user_id", "status");
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(users);
        JoinSource root = builder.root();
        JoinSource joined = builder.join(JoinType.LEFT, orders, root, "id", "user_id");
        JoinQuerySpec spec = builder.select(root, "name")
                                    .where(joined, ConditionGroup.and().where("status", "=", "paid").build())
                                    .orderBy(root, "id", PageSort.Direction.ASC)
                                    .build();
        Map<JoinSource, ConditionGroup> protections = Map.of(
                root, ConditionGroup.and().where("tenant_id", "=", "tenant-a").build());

        SqlRequest page = renderer().joinQueries().select(spec, protections, PageQuery.of(2, 20));
        SqlRequest count = renderer().joinQueries().count(spec, protections);

        assertEquals("select t0.name as s0_name from "
                             + "(select * from users where tenant_id = ?) t0 "
                             + "left outer join orders t1 on t0.id = t1.user_id "
                             + "where t1.status = ? order by t0.id asc limit ? offset ?",
                     page.sql());
        assertEquals(java.util.List.of("tenant-a", "paid", 20, 20L), page.parameters());
        assertEquals("select count(*) as total from "
                             + "(select * from users where tenant_id = ?) t0 "
                             + "left outer join orders t1 on t0.id = t1.user_id where t1.status = ?",
                     count.sql());
        assertEquals(java.util.List.of("tenant-a", "paid"), count.parameters());
    }

    /** JOIN 页码分页必须声明 source-qualified 稳定排序，不能依赖数据库未定义的返回顺序。 */
    @Test
    void rejectsJoinPageWithoutStableQualifiedOrder() {
        DynamicForm users = form("users", "id", "name");
        DynamicForm orders = form("orders", "id", "user_id");
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(users);
        JoinSource root = builder.root();
        JoinSource joined = builder.join(JoinType.LEFT, orders, root, "id", "user_id");
        JoinQuerySpec spec = builder.select(root, "name").select(joined, "id").build();

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> renderer().joinQueries().select(spec, Map.of(), PageQuery.of(1, 20)));

        assertEquals("join page requires at least one source-qualified order", error.getMessage());
    }

    /** 不同 JOIN 源的业务条件按 AND 合并时，源内顶层 OR 必须保持原分组语义。 */
    @Test
    void preservesTopLevelOrWhenCombiningJoinSourceConditions() {
        DynamicForm users = form("users", "id", "name");
        DynamicForm orders = form("orders", "id", "user_id", "status");
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(users);
        JoinSource root = builder.root();
        JoinSource joined = builder.join(JoinType.LEFT, orders, root, "id", "user_id");
        JoinQuerySpec spec = builder.select(root, "name")
                                    .where(root, ConditionGroup.or()
                                                               .where("name", "=", "alice")
                                                               .where("name", "=", "bob")
                                                               .build())
                                    .where(joined, ConditionGroup.and()
                                                                 .where("status", "=", "paid")
                                                                 .build())
                                    .build();

        SqlRequest request = renderer().joinQueries().select(spec, Map.of());

        assertEquals("select t0.name as s0_name from users t0 "
                             + "left outer join orders t1 on t0.id = t1.user_id "
                             + "where (t0.name = ? or t0.name = ?) and t1.status = ?",
                     request.sql());
        assertEquals(java.util.List.of("alice", "bob", "paid"), request.parameters());
    }

    /** relation-exists 的关系表结构标识符不能被 JOIN 当前源的字段限定规则误判。 */
    @Test
    void rendersRelationExistsInsideQualifiedJoinBusinessCondition() {
        DynamicForm users = form("users", "id", "name");
        DynamicForm orders = form("orders", "id", "user_id");
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(users);
        JoinSource root = builder.root();
        builder.join(JoinType.LEFT, orders, root, "id", "user_id");
        JoinQuerySpec spec = builder.select(root, "name")
                                    .where(root, ConditionGroup.and(RelationTermPackage.of(
                                            "user-org", "org_user", "ou", "user_id", "org_id",
                                            "user-in-org", "user-not-in-org").terms())
                                                               .where("id", "user-in-org", "org-1")
                                                               .build())
                                    .build();
        SqlRenderer conditionRenderer = SqlRenderer.builder()
                                                    .addDefaultTerms()
                                                    .addTermPackage(RelationTermPackage.of(
                                                            "user-org", "org_user", "ou", "user_id", "org_id",
                                                            "user-in-org", "user-not-in-org"))
                                                    .build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditionRenderer, RdbDialect.h2());

        SqlRequest request = renderer.joinQueries().select(spec, Map.of());

        assertEquals("select t0.name as s0_name from users t0 "
                             + "left outer join orders t1 on t0.id = t1.user_id "
                             + "where exists (select 1 from org_user ou "
                             + "where ou.user_id = t0.id and ou.org_id = ?)",
                     request.sql());
        assertEquals(java.util.List.of("org-1"), request.parameters());
    }

    /** JOIN 各数据源的标准条件也必须按字段和方言编码，而不是退回通用 Java codec。 */
    @Test
    void encodesJoinBusinessConditionThroughTheSourceDynamicField() {
        DynamicForm schedules = DynamicForm.builder("schedule", "schedules")
                                           .addField(DynamicField.primaryKey("id", "BIGINT"))
                                           .addField(DynamicField.of("meeting_time", "OFFSET_TIME"))
                                           .build();
        DynamicForm rooms = form("rooms", "id", "schedule_id");
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(schedules);
        JoinSource root = builder.root();
        builder.join(JoinType.LEFT, rooms, root, "id", "schedule_id");
        JoinQuerySpec spec = builder.select(root, "id")
                                    .where(root, ConditionGroup.and()
                                                               .where("meeting_time", "=",
                                                                      OffsetTime.parse("13:40:00+08:00"))
                                                               .build())
                                    .build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.mysql());

        SqlRequest request = renderer.joinQueries().select(spec, Map.of());

        assertEquals(List.of("13:40+08:00"), request.parameters());
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
    }

    private static DynamicForm form(String table, String... fields) {
        DynamicForm.Builder builder = DynamicForm.builder(table + "-form", table);
        for (String field : fields) {
            builder.addField(DynamicField.of(field, "VARCHAR"));
        }
        return builder.build();
    }
}
