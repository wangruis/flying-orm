package com.flying.orm.rdb.batch;

import com.flying.orm.core.sql.render.SqlIdentifiers;
import com.flying.orm.rdb.result.DynamicRow;

import java.util.Objects;

/**
 * 一次批量写入是否需要数据库生成键，以及生成键应该交给谁。
 *
 * <p>普通动态表单 Map 批量使用 {@link #none()}，不会增加驱动开销。实体声明 AUTO 或数据库序列主键时，
 * Repository 使用 {@link #required(String, BatchGeneratedKeyConsumer)} 明确要求执行内核逐行取得主键。
 * 生成键通过回调立即交还 Repository，不进入 BatchWriteResult，也不会随着批量结果长期占用内存。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
public final class BatchGeneratedKeys {

    private static final BatchGeneratedKeys NONE = new BatchGeneratedKeys(null, null);

    private final String columnName;
    private final BatchGeneratedKeyConsumer consumer;

    private BatchGeneratedKeys(String columnName, BatchGeneratedKeyConsumer consumer) {
        this.columnName = columnName;
        this.consumer = consumer;
    }

    /** @return 不要求生成键的共享配置。 */
    public static BatchGeneratedKeys none() {
        return NONE;
    }

    /**
     * 创建必须逐行返回一个主键的配置。
     *
     * @param columnName 数据库主键列名，不接受 SQL 片段
     * @param consumer 按整批输入偏移接收生成键的回调
     * @return 生成键配置
     */
    public static BatchGeneratedKeys required(String columnName, BatchGeneratedKeyConsumer consumer) {
        String safeColumn = SqlIdentifiers.requireIdentifier(columnName, "generated key column");
        return new BatchGeneratedKeys(safeColumn,
                                      Objects.requireNonNull(consumer, "generated key consumer must not be null"));
    }

    /** @return true 表示执行内核必须逐行取得并交付生成键。 */
    public boolean required() {
        return consumer != null;
    }

    /**
     * @return 必须返回的主键列名
     * @throws IllegalStateException 当前请求没有要求生成键
     */
    public String columnName() {
        if (!required()) {
            throw new IllegalStateException("batch write does not request generated keys");
        }
        return columnName;
    }

    /**
     * 由执行内核交付一行生成键。这里先检查偏移和结果，避免错误回调污染实体状态。
     */
    public void accept(long inputOffset, DynamicRow generatedKey) {
        if (!required()) {
            throw new IllegalStateException("batch write does not request generated keys");
        }
        if (inputOffset < 0L) {
            throw new IllegalArgumentException("generated key input offset must not be negative");
        }
        consumer.accept(inputOffset,
                        Objects.requireNonNull(generatedKey, "generated key row must not be null"));
    }
}
