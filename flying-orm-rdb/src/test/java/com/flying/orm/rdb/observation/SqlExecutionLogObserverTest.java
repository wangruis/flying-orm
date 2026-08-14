package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 SQL 日志默认不泄露业务数据，显式开启详情后仍受脱敏和长度预算约束。
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
class SqlExecutionLogObserverTest {

    /** 默认日志只保留排障需要的结构化字段，不带 SQL 正文和参数值。 */
    @Test
    void defaultsToStructuredMetadataWithoutSqlOrParameters() {
        List<String> messages = new ArrayList<>();
        SqlExecutionLogObserver observer = SqlExecutionLogObserver.create(
                SqlExecutionLogOptions.defaults(), messages::add);

        observer.onExecution(successfulUpdate());

        String message = messages.getFirst();
        assertTrue(message.contains("kind=SQL"));
        assertTrue(message.contains("statementType=UPDATE"));
        assertTrue(message.contains("affectedRows=1"));
        assertTrue(message.contains("parameterCount=2"));
        assertTrue(message.contains("transactionSource=AUTO_COMMIT"));
        assertFalse(message.contains("sql=\""));
        assertFalse(message.contains("parameters="));
        assertFalse(observer.requiresParameterValues());
    }

    /** SQL 和参数只有显式开启才展示，字符串内容会脱敏，整条消息也不能超过配置上限。 */
    @Test
    void masksEnabledDetailsAndHonorsMessageLimit() {
        List<String> messages = new ArrayList<>();
        SqlExecutionLogOptions options = SqlExecutionLogOptions.defaults()
                .withSql(true)
                .withParameters(true)
                .withLimits(32, 240);
        SqlExecutionLogObserver observer = SqlExecutionLogObserver.create(options, messages::add);

        observer.onExecution(successfulUpdate(),
                             List.of("secret-name", "user-100"),
                             SqlTransactionSource.EXTERNAL);

        String message = messages.getFirst();
        assertTrue(observer.requiresParameterValues());
        assertTrue(message.contains("transactionSource=EXTERNAL"));
        assertTrue(message.contains("sql=\"update Users set name = ?"));
        assertTrue(message.contains("parameters=[\"s*********e\", \"u******0\"]"));
        assertFalse(message.contains("secret-name"));
        assertFalse(message.contains("user-100"));
        assertTrue(message.length() <= 240);
    }

    /** 极小单值预算也必须是硬上限，不能被引号或省略号偷偷撑大。 */
    @Test
    void honorsVerySmallSingleValueLimit() {
        List<String> messages = new ArrayList<>();
        SqlExecutionLogObserver observer = SqlExecutionLogObserver.create(
                SqlExecutionLogOptions.defaults().withParameters(true).withLimits(3, 240), messages::add);

        observer.onExecution(successfulUpdate(), List.of("secret"));

        assertTrue(messages.getFirst().contains("parameters=[***]"));
    }

    /** 自定义规则只能把指定位置隐藏得更彻底；规则自身失败时也按完全隐藏处理。 */
    @Test
    void customRedactionCanOnlyStrengthenBuiltInProtection() {
        List<String> messages = new ArrayList<>();
        SqlExecutionLogOptions options = SqlExecutionLogOptions.defaults()
                .withParameters(true)
                .withRedactionRule((index, type) -> index == 0);
        SqlExecutionLogObserver observer = SqlExecutionLogObserver.create(options, messages::add);

        observer.onExecution(successfulUpdate(), List.of("secret-name", "user-100"));

        assertTrue(messages.getFirst().contains("parameters=[<masked>, \"u******0\"]"));
        SqlExecutionLogObserver failingRuleObserver = SqlExecutionLogObserver.create(
                options.withRedactionRule((index, type) -> {
                    throw new IllegalStateException("redaction unavailable");
                }),
                messages::add);
        failingRuleObserver.onExecution(successfulUpdate(), List.of("secret-name", "user-100"));
        assertTrue(messages.get(1).contains("parameters=[<masked>, <masked>]"));
    }

    /** PostgreSQL 的 dollar-quoted 文本也属于 SQL 字面量，显式记录 SQL 时不能把正文写进日志。 */
    @Test
    void masksPostgresqlDollarQuotedLiterals() {
        List<String> messages = new ArrayList<>();
        SqlExecutionLogObserver observer = SqlExecutionLogObserver.create(
                SqlExecutionLogOptions.defaults().withSql(true), messages::add);
        SqlExecutionObservation observation = new SqlExecutionObservation(
                SqlExecutionOperation.QUERY,
                SqlStatementType.SELECT,
                SqlExecutionStatus.SUCCESS,
                SqlFailureCategory.NONE,
                "select $$secret-value$$, $tag$another-secret$tag$",
                0,
                0,
                1,
                1,
                null);

        observer.onExecution(observation);

        String message = messages.getFirst();
        assertTrue(message.contains("$$***$$"));
        assertTrue(message.contains("$tag$***$tag$"));
        assertFalse(message.contains("secret-value"));
        assertFalse(message.contains("another-secret"));
    }

    /** Oracle 替代引号中的单引号不能提前结束脱敏区域并把后续文字写进日志。 */
    @Test
    void masksOracleAlternativeQuotedLiterals() {
        List<String> messages = new ArrayList<>();
        SqlExecutionLogObserver observer = SqlExecutionLogObserver.create(
                SqlExecutionLogOptions.defaults().withSql(true), messages::add);
        SqlExecutionObservation observation = new SqlExecutionObservation(
                SqlExecutionOperation.QUERY,
                SqlStatementType.SELECT,
                SqlExecutionStatus.SUCCESS,
                SqlFailureCategory.NONE,
                "select q''Mary's national-id-123456'' from dual",
                0,
                0,
                1,
                1,
                null);

        observer.onExecution(observation);

        String message = messages.getFirst();
        assertTrue(message.contains("q''***''"));
        assertFalse(message.contains("national-id-123456"));
    }

    /** 批量日志只读取结构化汇总，不接触批量参数 Publisher。 */
    @Test
    void writesBatchSummaryFromExistingObservation() {
        List<String> messages = new ArrayList<>();
        SqlExecutionLogObserver observer = SqlExecutionLogObserver.create(
                SqlExecutionLogOptions.defaults(), messages::add);
        BatchExecutionObservation observation = BatchExecutionObservation.summary(
                new BatchExecutionObservation.BatchWriteRequestView(
                        "insert into Users(id) values(?)", BatchWriteOptions.Mode.INDEPENDENT, 1),
                BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT,
                                      List.of(BatchChunkResult.committed(0, 0, 2, 2),
                                              BatchChunkResult.failed(1,
                                                                      2,
                                                                      1,
                                                                      new IllegalStateException("write failed")))),
                12L);

        observer.onExecution(observation);

        String message = messages.getFirst();
        assertTrue(message.contains("kind=BATCH"));
        assertTrue(message.contains("eventType=SUMMARY"));
        assertTrue(message.contains("status=PARTIAL"));
        assertTrue(message.contains("chunkCount=2"));
        assertTrue(message.contains("successfulChunkCount=1"));
        assertTrue(message.contains("failedChunkCount=1"));
        assertTrue(message.contains("parameterCount=1"));
        assertTrue(message.contains("transactionSource=INTERNAL"));
    }

    /** 连接清理失败只写固定结构化分类，并始终使用 WARN，不能复制驱动异常原文。 */
    @Test
    void writesSanitizedResourceCleanupAtWarnLevel() {
        List<String> messages = new ArrayList<>();
        List<SqlExecutionLogLevel> levels = new ArrayList<>();
        SqlExecutionLogObserver observer = SqlExecutionLogObserver.create(
                SqlExecutionLogOptions.defaults(), new SqlExecutionLogSink() {
                    @Override
                    public void write(String message) {
                        messages.add(message);
                    }

                    @Override
                    public void write(SqlExecutionLogLevel level, String message) {
                        levels.add(level);
                        messages.add(message);
                    }
                });

        observer.onResourceCleanup(new ResourceCleanupObservation(
                SqlExecutionOperation.CHUNKED_BATCH_WRITE,
                ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                true,
                new IllegalStateException("driver-secret-connection-details")));

        assertEquals(List.of(SqlExecutionLogLevel.WARN), levels);
        String message = messages.getFirst();
        assertTrue(message.contains("kind=RESOURCE_CLEANUP"));
        assertTrue(message.contains("operation=CHUNKED_BATCH_WRITE"));
        assertTrue(message.contains("phase=CONNECTION_CLOSE"));
        assertTrue(message.contains("outcomeConfirmed=true"));
        assertTrue(message.contains("failureKind=FAILURE"));
        assertFalse(message.contains("driver-secret-connection-details"));
    }

    /** 选择策略可以收起行数；耗时阈值只决定成功日志的级别，不会把 DEBUG 快 SQL误当成失败。 */
    @Test
    void selectionControlsSuccessFieldsAndSlowThreshold() {
        List<String> messages = new ArrayList<>();
        SqlExecutionLogSelection selection = new SqlExecutionLogSelection(false,
                                                                            false,
                                                                            false,
                                                                            200,
                                                                            true,
                                                                            true,
                                                                            true);
        SqlExecutionLogObserver observer = SqlExecutionLogObserver.create(
                SqlExecutionLogOptions.defaults(), selection, messages::add);

        observer.onExecution(successfulUpdate());
        observer.onExecution(slowSuccessfulQuery());

        assertEquals(2, messages.size());
        String message = messages.get(1);
        assertTrue(message.contains("kind=SQL"));
        assertFalse(message.contains("returnedRows="));
        assertFalse(message.contains("affectedRows="));
        assertFalse(message.contains("durationNanos="));
    }

    /** 错误不能因慢 SQL 阈值被隐藏，关闭 CHUNK 后只保留调用方需要的批量汇总。 */
    @Test
    void selectionKeepsFailuresAndFiltersBatchEventTypes() {
        List<String> messages = new ArrayList<>();
        SqlExecutionLogSelection selection = new SqlExecutionLogSelection(true,
                                                                            true,
                                                                            true,
                                                                            2,
                                                                            false,
                                                                            true,
                                                                            false);
        SqlExecutionLogObserver observer = SqlExecutionLogObserver.create(
                SqlExecutionLogOptions.defaults(), selection, messages::add);

        observer.onExecution(failedUpdate());
        observer.onExecution(chunkObservation());
        observer.onExecution(summaryObservation());

        assertTrue(messages.size() == 2);
        assertTrue(messages.get(0).contains("status=ERROR"));
        assertTrue(messages.get(1).contains("eventType=SUMMARY"));
    }

    /** 日志出口失效不能覆盖数据库已经产生的成功、失败或 UNKNOWN 结果。 */
    @Test
    void isolatesSinkFailure() {
        SqlExecutionLogObserver observer = SqlExecutionLogObserver.create(
                SqlExecutionLogOptions.defaults(), ignored -> {
                    throw new IllegalStateException("logger unavailable");
                });

        assertDoesNotThrow(() -> observer.onExecution(successfulUpdate()));
    }

    /** 脱敏规则用普通异常包装 VME 时，日志格式化不能把致命错误降级为“全部隐藏”。 */
    @Test
    void redactionRulePromotesNestedVirtualMachineError() {
        OutOfMemoryError fatal = new OutOfMemoryError("redaction nested fatal");
        IllegalStateException wrapper = new IllegalStateException("redaction wrapper", fatal);
        SqlExecutionLogObserver observer = SqlExecutionLogObserver.create(
                SqlExecutionLogOptions.defaults()
                                      .withParameters(true)
                                      .withRedactionRule((index, type) -> { throw wrapper; }),
                ignored -> { });

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                () -> observer.onExecution(successfulUpdate(), List.of("secret", "value")));

        assertSame(fatal, observed);
    }

    /** 脱敏扩展深层包装的 VME 也必须保持原对象，不能被诊断图节点预算静默吞掉。 */
    @Test
    void redactionRulePromotesDeeplyNestedVirtualMachineError() {
        OutOfMemoryError fatal = new OutOfMemoryError("redaction deeply nested fatal");
        RuntimeException wrapper = new IllegalStateException("wrapper-0", fatal);
        for (int depth = 1; depth < 70; depth++) {
            wrapper = new IllegalStateException("wrapper-" + depth, wrapper);
        }
        RuntimeException failure = wrapper;
        SqlExecutionLogObserver observer = SqlExecutionLogObserver.create(
                SqlExecutionLogOptions.defaults()
                                      .withParameters(true)
                                      .withRedactionRule((index, type) -> { throw failure; }),
                ignored -> { });

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                () -> observer.onExecution(successfulUpdate(), List.of("secret", "value")));

        assertSame(fatal, observed);
    }

    /** 日志 sink 用普通异常包装 VME 时，旁路隔离仍须传播原致命错误。 */
    @Test
    void logSinkPromotesNestedVirtualMachineError() {
        OutOfMemoryError fatal = new OutOfMemoryError("sink nested fatal");
        IllegalStateException wrapper = new IllegalStateException("sink wrapper", fatal);
        SqlExecutionLogObserver observer = SqlExecutionLogObserver.create(
                SqlExecutionLogOptions.defaults(), message -> { throw wrapper; });

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                                                  () -> observer.onExecution(successfulUpdate()));

        assertSame(fatal, observed);
    }

    private static SqlExecutionObservation successfulUpdate() {
        return new SqlExecutionObservation(SqlExecutionOperation.UPDATE,
                                           SqlStatementType.UPDATE,
                                           SqlExecutionStatus.SUCCESS,
                                           SqlFailureCategory.NONE,
                                           "update Users set name = ? where id = ? and password = 'secret'",
                                           2,
                                           0,
                                           1,
                                           100,
                                           null);
    }

    private static SqlExecutionObservation slowSuccessfulQuery() {
        return new SqlExecutionObservation(SqlExecutionOperation.QUERY,
                                           SqlStatementType.SELECT,
                                           SqlExecutionStatus.SUCCESS,
                                           SqlFailureCategory.NONE,
                                           "select id from Users",
                                           0,
                                           0,
                                           2,
                                           200,
                                           null);
    }

    private static SqlExecutionObservation failedUpdate() {
        return new SqlExecutionObservation(SqlExecutionOperation.UPDATE,
                                           SqlStatementType.UPDATE,
                                           SqlExecutionStatus.ERROR,
                                           SqlFailureCategory.BAD_SQL,
                                           "update Users set name = ?",
                                           1,
                                           0,
                                           0,
                                           1,
                                           new IllegalArgumentException("bad sql"));
    }

    private static BatchExecutionObservation chunkObservation() {
        return BatchExecutionObservation.chunk(new BatchExecutionObservation.BatchWriteRequestView(
                                               "insert into Users(id) values(?)", BatchWriteOptions.Mode.INDEPENDENT, 1),
                                               BatchChunkResult.committed(0, 0, 1, 1),
                                               1);
    }

    private static BatchExecutionObservation summaryObservation() {
        return BatchExecutionObservation.summary(new BatchExecutionObservation.BatchWriteRequestView(
                                                 "insert into Users(id) values(?)", BatchWriteOptions.Mode.INDEPENDENT, 1),
                                                 BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT,
                                                                       List.of(BatchChunkResult.committed(0, 0, 1, 1))),
                                                 2);
    }

}
