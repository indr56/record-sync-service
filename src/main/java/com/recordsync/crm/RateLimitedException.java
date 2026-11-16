package com.recordsync.crm;

/**
 * The CRM refused the call because we exceeded its rate limit (HTTP 429).
 * Not a failure of the record: wait for the suggested time and try again.
 */
public class RateLimitedException extends CrmException {

    private final long retryAfterMillis;

    public RateLimitedException(String message, long retryAfterMillis) {
        super(message);
        this.retryAfterMillis = retryAfterMillis;
    }

    public long retryAfterMillis() {
        return retryAfterMillis;
    }
}
