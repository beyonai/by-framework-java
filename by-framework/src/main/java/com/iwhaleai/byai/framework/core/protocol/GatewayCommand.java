package com.iwhaleai.byai.framework.core.protocol;

/**
 * Interface for all Gateway commands.
 * Using plain interface (not sealed) to allow user-defined command implementations in SDK examples.
 * For internal strict hierarchies where all implementations are known, consider using sealed interface.
 */
public interface GatewayCommand {

    String actionType();

    MessageHeader header();

    default String messageId() {
        return header().messageId();
    }

    default String sessionId() {
        return header().sessionId();
    }
}
