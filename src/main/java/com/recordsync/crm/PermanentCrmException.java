package com.recordsync.crm;

/**
 * A problem that retrying will not fix: invalid data rejected by the CRM, record not found,
 * permission denied (HTTP 4xx other than 429).
 */
public class PermanentCrmException extends CrmException {

    public PermanentCrmException(String message) {
        super(message);
    }
}
