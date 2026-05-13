package com.iwhaleai.byai.framework.common.exceptions;

/**
 * Exception thrown when an unsupported command type is received.
 */
public class UnsupportedCommandException extends FrameworkException {

    private final String commandType;

    public UnsupportedCommandException(String commandType) {
        super("Unsupported command type: " + commandType);
        this.commandType = commandType;
    }

    public String getCommandType() {
        return commandType;
    }
}
