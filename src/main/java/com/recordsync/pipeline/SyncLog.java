package com.recordsync.pipeline;

/**
 * Minimal console logging, switched on by the demo and off by default in tests.
 * A real service would use structured logging with tenant, record and trace ids.
 */
public final class SyncLog {

    private static volatile boolean enabled = false;

    private SyncLog() {
    }

    public static void setEnabled(boolean value) {
        enabled = value;
    }

    public static void info(String message) {
        if (enabled) {
            System.out.println("  [" + Thread.currentThread().getName() + "] " + message);
        }
    }
}
