package com.iwhaleai.byai.framework.common;

import redis.clients.jedis.params.XAddParams;

/**
 * Trimming policy for RedisStreamOps.xadd(). Trimming, when requested, is
 * always approximate ("~"): exact O(N) trimming is never exposed here since
 * it defeats the purpose of a streaming append and isn't something any
 * caller in this codebase needs.
 */
public final class XAddOptions {
    private final Long maxLen;

    private XAddOptions(Long maxLen) {
        this.maxLen = maxLen;
    }

    /** No trimming: the stream grows unbounded, matching xadd's default behavior. */
    public static XAddOptions noTrim() {
        return new XAddOptions(null);
    }

    /** Approximately trim the stream to around maxLen entries after this append. */
    public static XAddOptions trimTo(long maxLen) {
        return new XAddOptions(maxLen);
    }

    XAddParams toParams() {
        XAddParams params = XAddParams.xAddParams();
        if (maxLen != null) {
            params.maxLen(maxLen).approximateTrimming();
        }
        return params;
    }
}
