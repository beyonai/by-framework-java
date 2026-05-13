package com.iwhaleai.byai.framework.common.exceptions;

/**
 * Exception thrown when Redis connection fails.
 */
public class RedisConnectionException extends FrameworkException {

    public RedisConnectionException() {
        super("Failed to connect to Redis");
    }

    public RedisConnectionException(String message) {
        super(message);
    }

    public RedisConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
