package com.recordsync.crm;

/**
 * A temporary problem: timeout, connection reset, HTTP 5xx. Retrying later may succeed.
 */
public class TransientCrmException extends CrmException {

    public TransientCrmException(String message) {
        super(message);
    }
}
