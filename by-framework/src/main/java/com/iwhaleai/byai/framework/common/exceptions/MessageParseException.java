package com.iwhaleai.byai.framework.common.exceptions;

/**
 * Exception thrown when message parsing fails.
 */
public class MessageParseException extends FrameworkException {

    private final String messageId;

    public MessageParseException(String messageId) {
        super("Failed to parse message: " + messageId);
        this.messageId = messageId;
    }

    public MessageParseException(String messageId, Throwable cause) {
        super("Failed to parse message: " + messageId, cause);
        this.messageId = messageId;
    }

    public String getMessageId() {
        return messageId;
    }
}
