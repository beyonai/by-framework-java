package com.iwhaleai.byai.framework.core.protocol;

import lombok.Getter;

@Getter
public enum EventType {
    ANSWER_DELTA("answerDelta"),
    REASONING_LOG_DELTA("reasoningLogDelta"),
    REASONING_LOG_START("reasoningLogStart"),
    REASONING_LOG_END("reasoningLogEnd"),
    ARTIFACT("artifact"),
    FINAL_ANSWER("finalAnswer"),
    APP_STREAM_RESPONSE("appStreamResponse"),
    ERROR("error");

    private final String value;

    EventType(String value) {
        this.value = value;
    }
}
