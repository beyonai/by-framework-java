package com.iwhaleai.byai.framework.core.protocol;

/**
 * Cancel mode for task cancellation.
 */
public enum CancelMode {
    GRACEFUL("graceful"),
    FORCE("force");

    private final String value;

    CancelMode(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static CancelMode fromValue(String value) {
        if (value == null) {
            return GRACEFUL;
        }
        for (CancelMode mode : values()) {
            if (mode.value.equals(value)) {
                return mode;
            }
        }
        return GRACEFUL;
    }
}
