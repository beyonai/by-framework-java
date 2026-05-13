package com.iwhaleai.byai.framework.worker.sandbox;

/**
 * Context variable for tracking the current workspace directory.
 * Uses ThreadLocal for Java equivalent of Python's ContextVar.
 */
public class ActiveWorkspace {

    private static final ThreadLocal<String> WORKSPACE = new ThreadLocal<>();

    /**
     * Get the current workspace directory.
     */
    public static String get() {
        return WORKSPACE.get();
    }

    /**
     * Set the current workspace directory.
     */
    public static void set(String workspace) {
        WORKSPACE.set(workspace);
    }

    /**
     * Clear the current workspace.
     */
    public static void clear() {
        WORKSPACE.remove();
    }

    /**
     * Execute a Runnable with a specific workspace.
     */
    public static void executeWithWorkspace(String workspace, Runnable runnable) {
        String previous = WORKSPACE.get();
        try {
            WORKSPACE.set(workspace);
            runnable.run();
        } finally {
            if (previous != null) {
                WORKSPACE.set(previous);
            } else {
                WORKSPACE.remove();
            }
        }
    }
}
