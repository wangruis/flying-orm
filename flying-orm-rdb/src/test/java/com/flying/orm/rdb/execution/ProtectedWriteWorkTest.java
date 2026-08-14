package com.flying.orm.rdb.execution;

import com.flying.orm.core.sql.render.SqlRequest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证受保护写入工作单元在冷执行边界保存稳定的 owner 快照。
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
class ProtectedWriteWorkTest {

    /** 回执身份在构造和访问两端都必须复制内部数组，冷执行前不能被调用方改写。 */
    @Test
    void snapshotsArrayReceiptParametersAcrossTheColdExecutionBoundary() {
        byte[] receiptIdentity = {1, 2, 3};
        Object[] row = ProtectedBatchRows.extend(
                new Object[]{"ciphertext"}, null, new Object[]{receiptIdentity});

        receiptIdentity[0] = 9;
        Object[] exposed = ProtectedBatchRows.receiptParameters(row, 1);
        ((byte[]) exposed[0])[1] = 8;

        assertArrayEquals(new byte[]{1, 2, 3},
                          (byte[]) ProtectedBatchRows.receiptParameters(row, 1)[0]);
    }

    /** 回执参数中的嵌套数组节点也必须在 extend 时固定，不能等执行时才读取源对象。 */
    @Test
    void snapshotsNestedArrayReceiptGraphsAcrossTheColdExecutionBoundary() {
        byte[][] receiptIdentity = new byte[][]{{1, 2, 3}};
        Object[] row = ProtectedBatchRows.extend(
                new Object[]{"ciphertext"}, null, new Object[]{receiptIdentity});

        receiptIdentity[0][0] = 9;
        ((byte[][]) ProtectedBatchRows.receiptParameters(row, 1)[0])[0][1] = 8;

        assertArrayEquals(new byte[]{1, 2, 3},
                          ((byte[][]) ProtectedBatchRows.receiptParameters(row, 1)[0])[0]);
    }

    /** 构造后和访问器返回后修改数组，都不能改变工作单元保存的 owner。 */
    @Test
    void snapshotsArrayOwnerValuesAcrossTheColdExecutionBoundary() {
        byte[] owner = {1, 2, 3};
        ProtectedWriteWork work = work(Map.of("id", owner));

        owner[0] = 9;
        byte[] exposed = (byte[]) work.knownOwner().get("id");
        exposed[1] = 8;

        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) work.knownOwner().get("id"));
    }

    /** owner 的嵌套数组节点不能通过构造入参或公开访问器回写内部工作单元。 */
    @Test
    void snapshotsNestedArrayOwnerGraphsAcrossTheColdExecutionBoundary() {
        byte[][] owner = new byte[][]{{1, 2, 3}};
        ProtectedWriteWork work = work(Map.of("id", owner));

        owner[0][0] = 9;
        ((byte[][]) work.knownOwner().get("id"))[0][1] = 8;

        assertArrayEquals(new byte[]{1, 2, 3}, ((byte[][]) work.knownOwner().get("id"))[0]);
    }

    /** 数组图复制必须保持自引用结构且不能把源数组暴露给工作单元。 */
    @Test
    void preservesSelfReferencesInsideOwnedArrayGraphs() {
        Object[] owner = new Object[1];
        owner[0] = owner;
        ProtectedWriteWork work = work(Map.of("id", owner));

        Object[] snapshot = (Object[]) work.knownOwner().get("id");

        assertNotSame(owner, snapshot);
        assertSame(snapshot, snapshot[0]);
    }

    /** 显式 null 不代表数据库生成的 owner 已经可用。 */
    @Test
    void treatsNullInsertOwnerAsMissingGeneratedKey() {
        ProtectedWriteWork work = work(java.util.Collections.singletonMap("id", null));

        assertTrue(work.requiresGeneratedKeys());
    }

    /** 更新必须把 owner 查询捕获的主键集合重新附加到原始业务条件，不能让并发新增行漂移进写集合。 */
    @Test
    void narrowsProtectedUpdateToTheCapturedOwnerSet() {
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.UPDATE,
                new SqlRequest("update users set contact = ? where status = ?", List.of("cipher", "active")),
                new SqlRequest("select id from users where status = ?", List.of("active")),
                List.of("id"),
                Map.of(),
                "id = ?",
                "delete from users_tokens where id = ? and field_tag = ?",
                "insert into users_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{4}))));

        SqlRequest narrowed = work.writeRequestForOwners(List.of(Map.of("id", 7L), Map.of("id", 9L)));

        assertEquals("update users set contact = ? where status = ? and ((id = ?) or (id = ?))",
                     narrowed.sql());
        assertEquals(List.of("cipher", "active", 7L, 9L), narrowed.parameters());
    }

    /** owner 收敛参数必须留在 SQL Server 等数据库可安全接受的绑定上限内。 */
    @Test
    void rejectsProtectedUpdateOwnerSetBeyondSafeParameterLimit() {
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.UPDATE,
                new SqlRequest("update users set contact = ? where status = ?", List.of("cipher", "active")),
                new SqlRequest("select id from users where status = ?", List.of("active")),
                List.of("id"),
                Map.of(),
                "id = ?",
                "delete from users_tokens where id = ? and field_tag = ?",
                "insert into users_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{4}))));
        List<Map<String, Long>> owners = IntStream.range(0, 1_999)
                                                  .mapToObj(index -> Map.of("id", (long) index))
                                                  .toList();

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class, () -> work.writeRequestForOwners(owners));

        assertEquals("protected update owner set exceeds safe parameter limit", error.getMessage());
    }

    private static ProtectedWriteWork work(Map<String, Object> owner) {
        return new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into users(name) values (?)", List.of("alice")),
                null,
                List.of("id"),
                owner,
                "id = ?",
                "delete from users_tokens where id = ? and field_tag = ?",
                "insert into users_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{4}))));
    }
}
