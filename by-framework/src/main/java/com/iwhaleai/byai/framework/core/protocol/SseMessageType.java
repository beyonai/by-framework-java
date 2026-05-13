package com.iwhaleai.byai.framework.core.protocol;

/**
 * SSE message type enumeration for content types.
 */
public enum SseMessageType {
    TEXT("1002"),
    ECHART("2001"),
    FORM("2002"),
    DIGIT("2003"),
    IFRAME("2006"),
    TASK("2008");

    private final String value;

    SseMessageType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static SseMessageType fromValue(String value) {
        for (SseMessageType type : values()) {
            if (type.value.equals(value)) {
                return type;
            }
        }
        return TEXT; // default
    }
}
