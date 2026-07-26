package com.iwhaleai.byai.framework.core.protocol;

/**
 * Bidirectional codec for converting between domain content objects and wire-safe payloads.
 */
public interface ContentCodec {

    Object serialize(Object content);

    Object deserialize(Object content);
}
