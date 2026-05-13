package com.iwhaleai.byai.framework.util;

import java.util.UUID;

/**
 * Utility for generating message IDs.
 */
public final class MessageIdGenerator {

    private static final String MESSAGE_ID_PREFIX = "msg-";

    private MessageIdGenerator() {
        // Prevent instantiation
    }

    /**
     * Generate a new message ID.
     *
     * @return generated message ID in format "msg-{8-char-uuid}"
     */
    public static String generate() {
        return MESSAGE_ID_PREFIX + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }
}
