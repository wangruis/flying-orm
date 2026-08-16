package com.flying.orm.rdb.batch;

import com.flying.orm.rdb.codec.SqlTypedValue;
import com.flying.orm.rdb.execution.BatchRowSnapshotter;
import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.Type;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证批量写入配置的默认值和边界，不让危险参数拖到执行阶段才报错。
 *
 * @author wangr
 * @date 2026-07-23
 * @version v1.0
 */
class BatchWriteOptionsTest {

    /**
     * 默认入口必须走整批原子事务，独立模式需要使用方明确选择。
     */
    @Test
    void defaultsToAtomicAndRejectsUnsafeValues() {
        BatchWriteOptions defaults = BatchWriteOptions.defaults();

        assertEquals(BatchWriteOptions.Mode.ATOMIC, defaults.mode());
        assertEquals(500, defaults.chunkSize());
        assertEquals(1, defaults.concurrency());
        assertEquals(100_000L, defaults.maxRows());
        assertEquals(32L * 1024 * 1024, defaults.maxBufferedBytes());
        assertEquals(4_096, defaults.maxResultChunks());
        assertTrue(defaults.timeout().isPositive());
        assertEquals(BatchWriteOptions.RecoveryMode.NONE, defaults.recovery().mode());
        assertThrows(IllegalArgumentException.class, () -> BatchWriteOptions.atomic(0));
        assertThrows(IllegalArgumentException.class, () -> BatchWriteOptions.independent(100, 0));
    }

    @Test
    void unlimitedWaitingRequiresAnExplicitFactory() {
        BatchWriteOptions unlimited = BatchWriteOptions.unlimitedAtomic(100);

        assertEquals(Duration.ZERO, unlimited.timeout());
    }

    @Test
    void recoveryTokenCannotBypassReceiptTableValidation() {
        assertThrows(IllegalArgumentException.class, () -> new BatchChunkResult.RecoveryToken(
                "operation-1",
                0,
                "receipt; delete from users",
                "plan-hash",
                "payload-hash",
                1L,
                1L));
    }

    /**
     * 限制和回执配置通过新对象派生，原配置保持不变。
     */
    @Test
    void derivesLimitsAndReceiptWithoutChangingSource() {
        BatchWriteOptions source = BatchWriteOptions.atomic(100);
        BatchWriteOptions configured = source.withMaxRows(1_000)
                                             .withTimeout(Duration.ofSeconds(5))
                                             .withReceipt("order-import-1");

        assertEquals(100_000L, source.maxRows());
        assertEquals(1_000, configured.maxRows());
        assertEquals(Duration.ofSeconds(5), configured.timeout());
        assertEquals(BatchWriteOptions.RecoveryMode.RECEIPT, configured.recovery().mode());
        assertEquals("order-import-1", configured.recovery().operationId());
        assertThrows(IllegalArgumentException.class, () -> source.withReceipt(" "));
        assertThrows(IllegalArgumentException.class,
                     () -> new BatchWriteOptions.Recovery(BatchWriteOptions.RecoveryMode.RECEIPT,
                                                          "operation-1",
                                                          "receipt; drop table users",
                                                          Duration.ZERO));
    }

    /** 操作编号必须能放入项目提供的标准回执表，配置和外部恢复令牌不能把错误推迟到数据库。 */
    @Test
    void rejectsOperationIdsThatDoNotFitTheStandardReceiptSchema() {
        String oversized = "x".repeat(129);

        IllegalArgumentException configuration = assertThrows(
                IllegalArgumentException.class,
                () -> BatchWriteOptions.atomic(1).withReceipt(oversized));
        IllegalArgumentException token = assertThrows(
                IllegalArgumentException.class,
                () -> new BatchChunkResult.RecoveryToken(
                        oversized, 0, "flying_orm_batch_receipt", "plan", "payload", 1L, 1L));

        assertEquals("batch operation id must not exceed 128 characters", configuration.getMessage());
        assertEquals("batch operation id must not exceed 128 characters", token.getMessage());
        assertFalse(configuration.getMessage().contains(oversized));
    }

    @Test
    void derivesBoundedMemoryLimitsAndEstimatesLargeValues() {
        BatchWriteOptions configured = BatchWriteOptions.atomic(100)
                .withMemoryLimits(5_000, 8L * 1024 * 1024, 128);

        assertEquals(5_000L, configured.maxRows());
        assertEquals(8L * 1024 * 1024, configured.maxBufferedBytes());
        assertEquals(128, configured.maxResultChunks());
        assertTrue(BatchMemoryBudget.estimateRowBytes(new Object[]{"中文", new byte[128], 7L}) >= 168L);
        assertThrows(IllegalArgumentException.class,
                     () -> configured.withMemoryLimits(0, 1024, 1));
    }

    @Test
    void clientHardLimitsRejectOnlyRequestsThatTryToEnlargeTheBudget() {
        BatchMemoryLimits limits = new BatchMemoryLimits(1_000, 4, 20_000, 8L * 1024 * 1024, 200);

        limits.check(BatchWriteOptions.independent(500, 2)
                                            .withMemoryLimits(10_000, 4L * 1024 * 1024, 100));
        BatchMemoryLimitExceededException error = assertThrows(
                BatchMemoryLimitExceededException.class,
                () -> limits.check(BatchWriteOptions.independent(500, 8)));

        assertEquals("concurrency", error.limitName());
        assertEquals(4, error.limit());
        assertEquals(8, error.actual());
    }

    @Test
    void memoryEstimatorHandlesPrimitiveArraysAndSelfReferencingContainers() {
        ArrayList<Object> cyclic = new ArrayList<>();
        cyclic.add(cyclic);

        long bytes = BatchMemoryBudget.estimateRowBytes(new Object[]{new int[1_024], cyclic, "中文"});

        assertTrue(bytes >= 4_096L);
    }

    /** ByteBuffer 的剩余可读字节很小时，批量缓冲仍会强引用其完整容量，预算必须覆盖这部分保留内存。 */
    @Test
    void memoryEstimatorAccountsForRetainedByteBufferCapacity() {
        ByteBuffer buffer = ByteBuffer.allocate(8 * 1024 * 1024);
        buffer.position(buffer.capacity() - 1);

        assertTrue(BatchMemoryBudget.estimateValueBytes(buffer) > 1024 * 1024L);
    }

    /** 批量所有权边界必须把 tiny heap/direct slice 压成紧凑副本，不能继续强引用隐藏的大 backing。 */
    @Test
    void batchSnapshotCompactsByteBufferViews() {
        ByteBuffer heapRoot = ByteBuffer.allocate(8 * 1024 * 1024);
        heapRoot.position(heapRoot.capacity() - 1);
        ByteBuffer directRoot = ByteBuffer.allocateDirect(8 * 1024 * 1024);
        directRoot.position(directRoot.capacity() - 1);

        Object[] snapshot = BatchRowSnapshotter.snapshot(new Object[]{
                heapRoot.slice().asReadOnlyBuffer(), directRoot.slice()
        });

        assertEquals(1, ((ByteBuffer) snapshot[0]).capacity());
        assertEquals(1, ((ByteBuffer) snapshot[1]).capacity());
        assertTrue(BatchMemoryBudget.estimateRowBytes(snapshot) < 1_024L);
    }

    /** 任意精度数字的实际载荷不能绕过批量字节预算，尤其不能在回执哈希阶段再无界膨胀。 */
    @Test
    void memoryEstimatorAccountsForArbitraryPrecisionNumberPayloads() {
        BigInteger oneMegabyteInteger = BigInteger.ONE.shiftLeft(8 * 1024 * 1024);
        BigDecimal expandingDecimal = new BigDecimal(BigInteger.ONE, -40 * 1024 * 1024);

        assertTrue(BatchMemoryBudget.estimateValueBytes(oneMegabyteInteger) > 1024 * 1024L);
        assertTrue(BatchMemoryBudget.estimateValueBytes(expandingDecimal)
                           > BatchWriteOptions.DEFAULT_MAX_BUFFERED_BYTES);
    }

    /** LOB codec 的显式 SQL 类型外壳不能隐藏内部大值并绕过批量字节上限。 */
    @Test
    void memoryEstimatorIncludesExplicitSqlTypedValuePayload() {
        byte[] blob = new byte[2 * 1024 * 1024];
        SqlTypedValue value = new SqlTypedValue(SqlTypedValue.Kind.BLOB, blob);

        assertTrue(BatchMemoryBudget.estimateValueBytes(value) > blob.length);
    }

    /** R2DBC 显式参数同样是合法批量值，外壳不能隐藏其内部载荷。 */
    @Test
    void memoryEstimatorIncludesR2dbcParameterPayload() {
        byte[] blob = new byte[2 * 1024 * 1024];

        assertTrue(BatchMemoryBudget.estimateValueBytes(parameter(blob)) > blob.length);
    }

    /** 回执操作编号来自公开配置，重复操作的失败说明不能回显无界原始编号。 */
    @Test
    void receiptMismatchDoesNotExposeConfiguredOperationId() {
        String operationId = "private-operation-" + "x".repeat(5_000);

        BatchReceiptMismatchException error = new BatchReceiptMismatchException(operationId);

        assertEquals("batch receipt payload does not match the existing operation", error.getMessage());
        assertFalse(error.getMessage().contains(operationId));
    }

    /** 公开完整性异常只返回稳定分类，不能把调用方传入的任意长诊断文本复制进消息。 */
    @Test
    void receiptIntegrityFailureDoesNotExposeCallerDiagnostics() {
        String operation = "private-operation-" + "x".repeat(5_000);
        String expectation = "private-expectation-" + "y".repeat(5_000);
        String detail = "private-detail-" + "z".repeat(5_000);

        BatchReceiptIntegrityException rowCount =
                new BatchReceiptIntegrityException(operation, expectation, 0L);
        BatchReceiptIntegrityException conversion =
                new BatchReceiptIntegrityException(operation, detail, new IllegalArgumentException("codec"));

        assertEquals("batch receipt integrity check failed", rowCount.getMessage());
        assertEquals("batch receipt integrity check failed", conversion.getMessage());
        assertFalse(rowCount.getMessage().contains(operation));
        assertFalse(conversion.getMessage().contains(detail));
    }

    private static Parameter parameter(Object value) {
        Type type = new Type() {
            @Override
            public Class<?> getJavaType() {
                return value.getClass();
            }

            @Override
            public String getName() {
                return value.getClass().getName();
            }
        };
        return new Parameter() {
            @Override
            public Type getType() {
                return type;
            }

            @Override
            public Object getValue() {
                return value;
            }
        };
    }
}
