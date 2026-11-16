package com.recordsync.rules;

import com.recordsync.model.ChangeEvent;
import com.recordsync.model.SyncAction;

import java.util.List;

/**
 * Decides which CRUD action to perform in the CRM for one change.
 *
 * Two inputs drive the decision:
 * 1. The pre-configured rules: does any rule fire for this change at all?
 * 2. Whether the record already exists in the CRM (we know its external id).
 *
 * Mapping:
 * - created or updated, not yet in CRM  -> CREATE  (also covers records that existed before the
 *                                                   CRM was connected, or that only now meet a condition)
 * - created or updated, already in CRM  -> UPDATE
 * - deleted, already in CRM             -> DELETE
 * - deleted, never sent to CRM          -> SKIP
 * - no rule fires                       -> SKIP
 */
public class RuleEngine {

    private final List<SyncRule> rules;

    public RuleEngine(List<SyncRule> rules) {
        this.rules = List.copyOf(rules);
    }

    public SyncAction decide(ChangeEvent event, boolean existsInCrm) {
        boolean ruleFired = false;
        for (SyncRule rule : rules) {
            if (rule.matches(event)) {
                ruleFired = true;
                break;
            }
        }
        if (!ruleFired) {
            return SyncAction.SKIP;
        }

        switch (event.changeType()) {
            case CREATED:
            case UPDATED:
                return existsInCrm ? SyncAction.UPDATE : SyncAction.CREATE;
            case DELETED:
                return existsInCrm ? SyncAction.DELETE : SyncAction.SKIP;
            default:
                return SyncAction.SKIP;
        }
    }
}
