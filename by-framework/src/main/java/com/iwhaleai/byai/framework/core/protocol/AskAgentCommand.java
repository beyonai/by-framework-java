package com.iwhaleai.byai.framework.core.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

/**
 * Command for asking another agent.
 */
public record AskAgentCommand(
    @JsonProperty("action_type") String actionType,
    @JsonProperty("header") MessageHeader header,
    @JsonProperty("body") AskAgentBody body
) implements GatewayCommand {

    public AskAgentCommand {
        actionType = actionType != null ? actionType : ActionType.ASK_AGENT;
        header = Objects.requireNonNull(header, "header cannot be null");
        body = body != null ? body : new AskAgentBody(null, false, Map.of());
    }

    /**
     * Creates an AskAgentCommand with the given parameters.
     */
    public static AskAgentCommand of(
            MessageHeader header,
            Object content,
            boolean waitForReply,
            Map<String, Object> extraPayload
    ) {
        return new AskAgentCommand(
            ActionType.ASK_AGENT,
            header,
            new AskAgentBody(
                content,
                waitForReply,
                extraPayload != null ? extraPayload : Map.of()
            )
        );
    }

    /**
     * Creates a new builder for AskAgentCommand.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for AskAgentCommand.
     */
    public static final class Builder {
        private String actionType = ActionType.ASK_AGENT;
        private MessageHeader header;
        private AskAgentBody body;

        private Builder() {
        }

        public Builder actionType(String actionType) {
            this.actionType = actionType;
            return this;
        }

        public Builder header(MessageHeader header) {
            this.header = header;
            return this;
        }

        public Builder body(AskAgentBody body) {
            this.body = body;
            return this;
        }

        public AskAgentCommand build() {
            return new AskAgentCommand(actionType, header, body);
        }
    }

    /**
     * Returns the content of this ask command.
     */
    public Object content() {
        if (body == null) {
            return "";
        }
        Object c = body.content();
        return c != null ? c : "";
    }

    /**
     * Returns whether to wait for reply.
     */
    public boolean waitForReply() {
        return body != null && body.waitForReply();
    }

    /**
     * Returns the extra payload of this ask command.
     */
    public Map<String, Object> extraPayload() {
        if (body == null) {
            return Map.of();
        }
        Map<String, Object> ep = body.extraPayload();
        return ep != null ? ep : Map.of();
    }

    /**
     * Immutable body record for AskAgentCommand.
     */
    public record AskAgentBody(
        @JsonProperty("content") Object content,
        @JsonProperty("wait_for_reply") boolean waitForReply,
        @JsonProperty("extra_payload") Map<String, Object> extraPayload
    ) {
        public AskAgentBody {
            extraPayload = extraPayload != null ? extraPayload : Map.of();
        }
    }
}
