package com.iwhaleai.byai.framework.common.exceptions;

/**
 * Exception thrown when session mismatch is detected.
 */
public class SessionMismatchException extends FrameworkException {

    private final String messageId;
    private final String expectedSession;
    private final String actualSession;

    public SessionMismatchException(String messageId, String expectedSession, String actualSession) {
        super("Session mismatch for message " + messageId + ": expected " + expectedSession + ", got " + actualSession);
        this.messageId = messageId;
        this.expectedSession = expectedSession;
        this.actualSession = actualSession;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getExpectedSession() {
        return expectedSession;
    }

    public String getActualSession() {
        return actualSession;
    }
}
