package com.recordsync.rules;

import com.recordsync.demo.SampleConfig;
import com.recordsync.model.ChangeType;
import com.recordsync.model.SyncAction;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.List;

import static com.recordsync.TestEvents.contact;
import static com.recordsync.TestEvents.delete;
import static com.recordsync.TestEvents.event;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RuleEngineTest {

    private final RuleEngine syncEnabledOnly = new RuleEngine(List.of(
            SyncRule.when(SampleConfig.CONTACT, EnumSet.allOf(ChangeType.class), "sync_enabled", true)));

    @Test
    void createWhenRecordIsNotInCrmYet() {
        assertEquals(SyncAction.CREATE,
                syncEnabledOnly.decide(event("c-1", ChangeType.CREATED, 1, contact("A", 1)), false));
    }

    @Test
    void updateOfRecordNotInCrmBecomesCreate() {
        // For example, the record existed before the CRM was connected.
        assertEquals(SyncAction.CREATE,
                syncEnabledOnly.decide(event("c-1", ChangeType.UPDATED, 5, contact("A", 1)), false));
    }

    @Test
    void updateWhenRecordIsInCrm() {
        assertEquals(SyncAction.UPDATE,
                syncEnabledOnly.decide(event("c-1", ChangeType.UPDATED, 2, contact("A", 1)), true));
    }

    @Test
    void deleteOnlyWhenRecordIsInCrm() {
        assertEquals(SyncAction.DELETE, syncEnabledOnly.decide(delete("c-1", 3), true));
        assertEquals(SyncAction.SKIP, syncEnabledOnly.decide(delete("c-1", 3), false));
    }

    @Test
    void skipWhenConditionDoesNotMatch() {
        assertEquals(SyncAction.SKIP,
                syncEnabledOnly.decide(event("c-1", ChangeType.CREATED, 1, contact("A", "a@acme.com", 1, false)), false));
    }

    @Test
    void skipWhenTriggerDoesNotIncludeTheChangeType() {
        RuleEngine createOnly = new RuleEngine(List.of(
                SyncRule.always(SampleConfig.CONTACT, EnumSet.of(ChangeType.CREATED))));

        assertEquals(SyncAction.SKIP, createOnly.decide(event("c-1", ChangeType.UPDATED, 2, contact("A", 1)), true));
    }

    @Test
    void skipWhenNoRuleForObjectType() {
        RuleEngine accountsOnly = new RuleEngine(List.of(
                SyncRule.always("account", EnumSet.allOf(ChangeType.class))));

        assertEquals(SyncAction.SKIP, accountsOnly.decide(event("c-1", ChangeType.CREATED, 1, contact("A", 1)), false));
    }
}
