package com.iwhaleai.byai.framework.core.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

/**
 * Command for resuming a paused task or returning agent result.
 */
public record ResumeCommand(
    @JsonProperty("action_type") String actionType,
    @JsonProperty("header") MessageHeader header,
    @JsonProperty("body") ResumeBody body
) implements GatewayCommand {

    public ResumeCommand {
        actionType = actionType != null ? actionType : ActionType.RESUME;
        header = Objects.requireNonNull(header, "header cannot be null");
        body = body != null ? body : new ResumeBody(null, "", null, Map.of());
    }

    /**
     * Creates a ResumeCommand with the given parameters.
     */
    public static ResumeCommand of(
            MessageHeader header,
            Object content,
            String status,
            Object replyData,
            Map<String, Object> extraPayload
    ) {
        return new ResumeCommand(
            ActionType.RESUME,
            header,
            new ResumeBody(
                content,
                status != null ? status : "",
                replyData,
                extraPayload != null ? extraPayload : Map.of()
            )
        );
    }

    /**
     * Creates a new builder for ResumeCommand.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for ResumeCommand.
     */
    public static final class Builder {
        private String actionType = ActionType.RESUME;
        private MessageHeader header;
        private ResumeBody body;

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

        public Builder body(ResumeBody body) {
            this.body = body;
            return this;
        }

        public ResumeCommand build() {
            return new ResumeCommand(actionType, header, body);
        }
    }

    /**
     * Returns the content of this resume command.
     */
    public Object content() {
        if (body == null) {
            return "";
        }
        Object c = body.content();
        return c != null ? c : "";
    }

    /**
     * Returns the status of this resume command.
     */
    public String status() {
        if (body == null) {
            return "";
        }
        String s = body.status();
        return s != null ? s : "";
    }

    /**
     * Returns the reply data of this resume command.
     */
    public Object replyData() {
        if (body == null) {
            return null;
        }
        return body.replyData();
    }

    /**
     * Returns the extra payload of this resume command.
     */
    public Map<String, Object> extraPayload() {
        if (body == null) {
            return Map.of();
        }
        Map<String, Object> ep = body.extraPayload();
        return ep != null ? ep : Map.of();
    }

    /**
     * Immutable body record for ResumeCommand.
     */
    public record ResumeBody(
        @JsonProperty("content") Object content,
        @JsonProperty("status") String status,
        @JsonProperty("reply_data") Object replyData,
        @JsonProperty("extra_payload") Map<String, Object> extraPayload
    ) {
        public ResumeBody {
            status = status != null ? status : "";
            extraPayload = extraPayload != null ? extraPayload : Map.of();
        }
    }
}
