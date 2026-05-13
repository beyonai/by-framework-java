package com.iwhaleai.byai.framework.core.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Command for cancelling a running task.
 */
public record CancelTaskCommand(
    @JsonProperty("action_type") String actionType,
    @JsonProperty("header") MessageHeader header,
    @JsonProperty("body") CancelTaskBody body
) implements GatewayCommand {

    public CancelTaskCommand {
        actionType = actionType != null ? actionType : ActionType.CANCEL_TASK;
        header = Objects.requireNonNull(header, "header cannot be null");
        body = body != null ? body : new CancelTaskBody(null, "", "", "", "", "graceful");
    }

    /**
     * Creates a new builder for CancelTaskCommand.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the target message ID for cancellation.
     */
    public String targetMessageId() {
        if (body == null) {
            return "";
        }
        String id = body.targetMessageId();
        return id != null ? id : "";
    }

    /**
     * Builder for CancelTaskCommand.
     */
    public static final class Builder {
        private String actionType = ActionType.CANCEL_TASK;
        private MessageHeader header;
        private CancelTaskBody body;

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

        public Builder body(CancelTaskBody body) {
            this.body = body;
            return this;
        }

        public CancelTaskCommand build() {
            return new CancelTaskCommand(actionType, header, body);
        }
    }

    /**
     * Immutable body record for CancelTaskCommand.
     */
    public record CancelTaskBody(
        @JsonProperty("target_message_id") String targetMessageId,
        @JsonProperty("target_execution_id") String targetExecutionId,
        @JsonProperty("target_worker_id") String targetWorkerId,
        @JsonProperty("reason") String reason,
        @JsonProperty("requested_by") String requestedBy,
        @JsonProperty("cancel_mode") String cancelMode
    ) {
        public CancelTaskBody {
            targetExecutionId = targetExecutionId != null ? targetExecutionId : "";
            targetWorkerId = targetWorkerId != null ? targetWorkerId : "";
            reason = reason != null ? reason : "";
            requestedBy = requestedBy != null ? requestedBy : "";
            cancelMode = cancelMode != null ? cancelMode : "graceful";
        }

        /**
         * Creates a new builder for CancelTaskBody.
         */
        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private String targetMessageId;
            private String targetExecutionId = "";
            private String targetWorkerId = "";
            private String reason = "";
            private String requestedBy = "";
            private String cancelMode = "graceful";

            private Builder() {
            }

            public Builder targetMessageId(String targetMessageId) {
                this.targetMessageId = targetMessageId;
                return this;
            }

            public Builder targetExecutionId(String targetExecutionId) {
                this.targetExecutionId = targetExecutionId;
                return this;
            }

            public Builder targetWorkerId(String targetWorkerId) {
                this.targetWorkerId = targetWorkerId;
                return this;
            }

            public Builder reason(String reason) {
                this.reason = reason;
                return this;
            }

            public Builder requestedBy(String requestedBy) {
                this.requestedBy = requestedBy;
                return this;
            }

            public Builder cancelMode(String cancelMode) {
                this.cancelMode = cancelMode;
                return this;
            }

            public CancelTaskBody build() {
                return new CancelTaskBody(
                    targetMessageId, targetExecutionId, targetWorkerId,
                    reason, requestedBy, cancelMode
                );
            }
        }
    }
}
