package com.iwhaleai.byai.framework.core.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Command to trigger an ordered plugin reload on a worker.
 */
public record ReloadPluginsCommand(
    @JsonProperty("action_type") String actionType,
    @JsonProperty("header") MessageHeader header,
    @JsonProperty("body") ReloadPluginsBody body
) implements GatewayCommand {

    public ReloadPluginsCommand {
        actionType = actionType != null ? actionType : ActionType.RELOAD_PLUGINS;
        header = Objects.requireNonNull(header, "header cannot be null");
        body = body != null ? body : new ReloadPluginsBody("", "");
    }

    public static ReloadPluginsCommand of(MessageHeader header, String reloadId, String reason) {
        return new ReloadPluginsCommand(ActionType.RELOAD_PLUGINS, header, new ReloadPluginsBody(reloadId, reason));
    }

    public String reloadId() {
        return body.reloadId();
    }

    public String reason() {
        return body.reason();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String actionType = ActionType.RELOAD_PLUGINS;
        private MessageHeader header;
        private ReloadPluginsBody body;

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

        public Builder body(ReloadPluginsBody body) {
            this.body = body;
            return this;
        }

        public ReloadPluginsCommand build() {
            return new ReloadPluginsCommand(actionType, header, body);
        }
    }

    /**
     * Immutable body record for ReloadPluginsCommand.
     */
    public record ReloadPluginsBody(
        @JsonProperty("reload_id") String reloadId,
        @JsonProperty("reason") String reason
    ) {
        public ReloadPluginsBody {
            reloadId = reloadId != null ? reloadId : "";
            reason = reason != null ? reason : "";
        }
    }
}
