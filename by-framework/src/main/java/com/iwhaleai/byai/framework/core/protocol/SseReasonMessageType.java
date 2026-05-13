package com.iwhaleai.byai.framework.core.protocol;

/**
 * SSE reason message type enumeration for reasoning/thinking content.
 */
public enum SseReasonMessageType {
    THINK_TITLE("3003"),
    THINK_SUB_TITLE("3005"),
    THINK_RESOURCE("3004"),
    THINK_TEXT("1002"),
    THINK_CODE_ANSWER("3008"),
    THINK_CODE("3006"),
    THINK_CODE_RESULT("3007"),
    TASK_FINISHED("3009"),
    TASK_USER_INPUT("3013"),
    TASK_CREATE_FILE("3010"),
    TASK_TITLE("3011"),
    AGENT_CARD("2015"),
    ASYNC_CARD("2014");

    private final String value;

    SseReasonMessageType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static SseReasonMessageType fromValue(String value) {
        for (SseReasonMessageType type : values()) {
            if (type.value.equals(value)) {
                return type;
            }
        }
        return THINK_TEXT; // default
    }
}
