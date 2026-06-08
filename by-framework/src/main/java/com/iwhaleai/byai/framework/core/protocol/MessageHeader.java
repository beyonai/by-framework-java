package com.iwhaleai.byai.framework.core.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

/**
 * Immutable message header record.
 * Uses Java record for type-safe, immutable data carrier.
 */
public record MessageHeader(
    @JsonProperty("message_id") String messageId,
    @JsonProperty("session_id") String sessionId,
    @JsonProperty("trace_id") String traceId,
    @JsonProperty("source_agent_type") String sourceAgentType,
    @JsonProperty("target_agent_type") String targetAgentType,
    @JsonProperty("parent_message_id") String parentMessageId,
    @JsonProperty("task_group_id") String taskGroupId,
    @JsonProperty("user_code") String userCode,
    @JsonProperty("user_name") String userName,
    @JsonProperty("trace_parent_span_id") String traceParentSpanId,
    @JsonProperty("langfuse_parent_observation_id") String langfuseParentObservationId,
    @JsonProperty("metadata") Map<String, Object> metadata
) {
    /**
     * Compact constructor with null-safety and default value normalization.
     */
    public MessageHeader {
        Objects.requireNonNull(sessionId, "sessionId cannot be null");
        traceId = traceId != null ? traceId : "";
        sourceAgentType = sourceAgentType != null ? sourceAgentType : "";
        targetAgentType = targetAgentType != null ? targetAgentType : "";
        parentMessageId = parentMessageId != null ? parentMessageId : "";
        taskGroupId = taskGroupId != null ? taskGroupId : "";
        userCode = userCode != null ? userCode : "";
        userName = userName != null ? userName : "";
        traceParentSpanId = traceParentSpanId != null ? traceParentSpanId : "";
        langfuseParentObservationId = langfuseParentObservationId != null ? langfuseParentObservationId : "";
        metadata = metadata != null ? metadata : Map.of();
    }

    /**
     * Creates a new builder for MessageHeader.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder pattern for MessageHeader construction.
     */
    public static final class Builder {
        private String messageId;
        private String sessionId = "";
        private String traceId = "";
        private String sourceAgentType = "";
        private String targetAgentType = "";
        private String parentMessageId = "";
        private String taskGroupId = "";
        private String userCode = "";
        private String userName = "";
        private String traceParentSpanId = "";
        private String langfuseParentObservationId = "";
        private Map<String, Object> metadata = Map.of();

        private Builder() {
        }

        public Builder messageId(String messageId) {
            this.messageId = messageId;
            return this;
        }

        public Builder sessionId(String sessionId) {
            this.sessionId = sessionId;
            return this;
        }

        public Builder traceId(String traceId) {
            this.traceId = traceId;
            return this;
        }

        public Builder sourceAgentType(String sourceAgentType) {
            this.sourceAgentType = sourceAgentType;
            return this;
        }

        public Builder targetAgentType(String targetAgentType) {
            this.targetAgentType = targetAgentType;
            return this;
        }

        public Builder parentMessageId(String parentMessageId) {
            this.parentMessageId = parentMessageId;
            return this;
        }

        public Builder taskGroupId(String taskGroupId) {
            this.taskGroupId = taskGroupId;
            return this;
        }

        public Builder userCode(String userCode) {
            this.userCode = userCode;
            return this;
        }

        public Builder userName(String userName) {
            this.userName = userName;
            return this;
        }

        public Builder traceParentSpanId(String traceParentSpanId) {
            this.traceParentSpanId = traceParentSpanId;
            return this;
        }

        public Builder langfuseParentObservationId(String langfuseParentObservationId) {
            this.langfuseParentObservationId = langfuseParentObservationId;
            return this;
        }

        public Builder metadata(Map<String, Object> metadata) {
            this.metadata = metadata != null ? metadata : Map.of();
            return this;
        }

        public MessageHeader build() {
            return new MessageHeader(
                messageId, sessionId, traceId, sourceAgentType, targetAgentType,
                parentMessageId, taskGroupId, userCode, userName,
                traceParentSpanId, langfuseParentObservationId, metadata
            );
        }
    }
}
