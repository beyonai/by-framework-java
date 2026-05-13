package com.iwhaleai.byai.framework.common.exceptions;

/**
 * Exception thrown when command validation fails.
 */
public class CommandValidationException extends FrameworkException {

    private final String commandType;
    private final String reason;

    public CommandValidationException(String commandType, String reason) {
        super("Command validation failed for " + commandType + ": " + reason);
        this.commandType = commandType;
        this.reason = reason;
    }

    public String getCommandType() {
        return commandType;
    }

    public String getReason() {
        return reason;
    }
}
