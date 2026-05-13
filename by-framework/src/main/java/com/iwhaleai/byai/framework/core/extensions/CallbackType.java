package com.iwhaleai.byai.framework.core.extensions;

public enum CallbackType {
    BEFORE_MODEL_CALLBACK("before_model_callback"),
    AFTER_MODEL_CALLBACK("after_model_callback"),
    BEFORE_TOOL_CALLBACK("before_tool_callback"),
    AFTER_TOOL_CALLBACK("after_tool_callback"),
    BEFORE_AGENT_CALLBACK("before_agent_callback"),
    AFTER_AGENT_CALLBACK("after_agent_callback");

    private final String value;

    CallbackType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
