package com.iwhaleai.byai.framework.core.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Admin command: resume a previously suspended worker.
 */
public record ResumeWorkerCommand(
    @JsonProperty("action_type") String actionType,
    @JsonProperty("header") MessageHeader header,
    @JsonProperty("body") ResumeWorkerBody body
) implements GatewayCommand {

    public ResumeWorkerCommand {
        actionType = actionType != null ? actionType : ActionType.RESUME_WORKER;
        header = Objects.requireNonNull(header, "header cannot be null");
        body = body != null ? body : new ResumeWorkerBody();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String actionType = ActionType.RESUME_WORKER;
        private MessageHeader header;
        private ResumeWorkerBody body = new ResumeWorkerBody();

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

        public Builder body(ResumeWorkerBody body) {
            this.body = body;
            return this;
        }

        public ResumeWorkerCommand build() {
            return new ResumeWorkerCommand(actionType, header, body);
        }
    }

    /**
     * Empty body record for ResumeWorkerCommand.
     */
    public record ResumeWorkerBody() {
    }
}
