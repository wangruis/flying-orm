package com.flying.orm.rdb.form;

import com.flying.orm.core.error.OrmErrorReport;
import com.flying.orm.core.error.OrmErrorReportProvider;

/**
 * CONTAINS 盲索引候选超过受控上限，结果不能通过截断伪装为完整命中。
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
public final class ProtectedSearchCandidateLimitExceededException extends IllegalStateException
        implements OrmErrorReportProvider {

    private final int limit;
    private final int actual;

    ProtectedSearchCandidateLimitExceededException(int limit, int actual) {
        super("protected search candidate limit exceeded");
        this.limit = limit;
        this.actual = actual;
    }

    /** @return 本次查询允许复核的最大候选数。 */
    public int limit() {
        return limit;
    }

    /** @return 检测到的候选数；通常为上限加一。 */
    public int actual() {
        return actual;
    }

    @Override
    public OrmErrorReport toErrorReport() {
        return new OrmErrorReport("PROTECTED_FIELD", "PROTECTED_SEARCH_CANDIDATE_LIMIT",
                                  null, null, null, getMessage());
    }
}
