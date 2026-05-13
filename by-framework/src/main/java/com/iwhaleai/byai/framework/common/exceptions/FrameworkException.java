package com.iwhaleai.byai.framework.common.exceptions;

/**
 * Base exception class for By-Framework.
 * Provides an error code field for structured error identification.
 */
public class FrameworkException extends RuntimeException {

    private final Throwable cause;
    private final String code;

    public FrameworkException(String message) {
        super(message);
        this.cause = null;
        this.code = getClass().getSimpleName();
    }

    public FrameworkException(String message, Throwable cause) {
        super(message, cause);
        this.cause = cause;
        this.code = getClass().getSimpleName();
    }

    public FrameworkException(String message, Throwable cause, String code) {
        super(message, cause);
        this.cause = cause;
        this.code = code != null ? code : getClass().getSimpleName();
    }

    public String getCode() {
        return code;
    }

    @Override
    public Throwable getCause() {
        return cause;
    }
}
