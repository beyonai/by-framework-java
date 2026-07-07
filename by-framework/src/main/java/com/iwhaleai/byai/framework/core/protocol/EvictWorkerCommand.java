package com.iwhaleai.byai.framework.core.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Admin command: shut down a worker after draining in-flight tasks.
 *
 * When force=true the worker cancels in-flight tasks immediately instead
 * of waiting for them to finish.
 */
public record EvictWorkerCommand(
    @JsonProperty("action_type") String actionType,
    @JsonProperty("header") MessageHeader header,
    @JsonProperty("body") EvictWorkerBody body
) implements GatewayCommand {

    public EvictWorkerCommand {
        actionType = actionType != null ? actionType : ActionType.EVICT_WORKER;
        header = Objects.requireNonNull(header, "header cannot be null");
        body = body != null ? body : new EvictWorkerBody("", false);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String actionType = ActionType.EVICT_WORKER;
        private MessageHeader header;
        private EvictWorkerBody body;

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

        public Builder body(EvictWorkerBody body) {
            this.body = body;
            return this;
        }

        public EvictWorkerCommand build() {
            return new EvictWorkerCommand(actionType, header, body);
        }
    }

    /**
     * Immutable body record for EvictWorkerCommand.
     */
    public record EvictWorkerBody(
        @JsonProperty("reason") String reason,
        @JsonProperty("force") boolean force
    ) {
        public EvictWorkerBody {
            reason = reason != null ? reason : "";
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private String reason = "";
            private boolean force = false;

            private Builder() {
            }

            public Builder reason(String reason) {
                this.reason = reason;
                return this;
            }

            public Builder force(boolean force) {
                this.force = force;
                return this;
            }

            public EvictWorkerBody build() {
                return new EvictWorkerBody(reason, force);
            }
        }
    }
}
