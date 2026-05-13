package com.iwhaleai.byai.framework.core.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataMessage {
    @JsonProperty("message_id")
    private String messageId;
    @JsonProperty("trace_id")
    private String traceId;
    @JsonProperty("session_id")
    private String sessionId;
    @JsonProperty("event_type")
    private String eventType;
    @JsonProperty("source_agent_type")
    private String sourceAgentType;
    @JsonProperty("parent_message_id")
    private String parentMessageId;
    @JsonProperty("parent_order_id")
    private String parentOrderId;
    private Map<String, Object> data;
    @JsonProperty("state_msg")
    private String stateMsg;
    @JsonProperty("artifact_url")
    private String artifactUrl;
    private Map<String, Object> metadata;
}
