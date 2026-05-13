package com.iwhaleai.byai.framework.common.exceptions;

/**
 * Exception thrown when message data is not found.
 */
public class MessageDataNotFoundException extends FrameworkException {

    private final String messageId;

    public MessageDataNotFoundException(String messageId) {
        super("Message data not found: " + messageId);
        this.messageId = messageId;
    }

    public String getMessageId() {
        return messageId;
    }
}
