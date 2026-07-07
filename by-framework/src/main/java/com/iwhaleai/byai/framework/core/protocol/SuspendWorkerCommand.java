package com.iwhaleai.byai.framework.core.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Admin command: pause a running worker from consuming new tasks.
 *
 * In-flight executions continue to completion. The worker resumes only
 * when a ResumeWorkerCommand arrives or the process restarts.
 */
public record SuspendWorkerCommand(
    @JsonProperty("action_type") String actionType,
    @JsonProperty("header") MessageHeader header,
    @JsonProperty("body") SuspendWorkerBody body
) implements GatewayCommand {

    public SuspendWorkerCommand {
        actionType = actionType != null ? actionType : ActionType.SUSPEND_WORKER;
        header = Objects.requireNonNull(header, "header cannot be null");
        body = body != null ? body : new SuspendWorkerBody("");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String actionType = ActionType.SUSPEND_WORKER;
        private MessageHeader header;
        private SuspendWorkerBody body;

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

        public Builder body(SuspendWorkerBody body) {
            this.body = body;
            return this;
        }

        public SuspendWorkerCommand build() {
            return new SuspendWorkerCommand(actionType, header, body);
        }
    }

    /**
     * Immutable body record for SuspendWorkerCommand.
     */
    public record SuspendWorkerBody(
        @JsonProperty("reason") String reason
    ) {
        public SuspendWorkerBody {
            reason = reason != null ? reason : "";
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private String reason = "";

            private Builder() {
            }

            public Builder reason(String reason) {
                this.reason = reason;
                return this;
            }

            public SuspendWorkerBody build() {
                return new SuspendWorkerBody(reason);
            }
        }
    }
}
