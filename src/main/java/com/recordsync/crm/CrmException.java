package com.recordsync.crm;

/**
 * Base class for errors returned by a CRM. The subclass tells the pipeline how to react:
 * {@link RateLimitedException} wait and try again, {@link TransientCrmException} retry with
 * backoff, {@link PermanentCrmException} stop and send to the dead-letter queue.
 */
public abstract class CrmException extends RuntimeException {

    protected CrmException(String message) {
        super(message);
    }
}
