package com.iwhaleai.byai.framework.core.protocol;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ByaiContentCodecTest {

    private final ByaiContentCodec codec = new ByaiContentCodec();

    @Test
    void deserializePassesPlainStringThrough() {
        assertEquals("hello", codec.deserialize("hello"));
    }

    @Test
    void deserializePassesNonMessageArrayThrough() {
        List<Map<String, Object>> content = List.of(Map.of("foo", "bar"));
        assertSame(content, codec.deserialize(content));
    }

    @Test
    void deserializePassesEmptyArrayThrough() {
        List<Object> content = List.of();
        assertEquals(content, codec.deserialize(content));
    }

    @Test
    void deserializeUnwrapsSingleMessageArrayToScalarMessage() {
        Map<String, Object> wireContent = new HashMap<>();
        wireContent.put("text", "hi");
        Map<String, Object> wireMessage = new HashMap<>();
        wireMessage.put("role", "user");
        wireMessage.put("content", wireContent);

        Object result = codec.deserialize(List.of(wireMessage));

        assertInstanceOf(BaiYingMessage.class, result);
        BaiYingMessage message = (BaiYingMessage) result;
        assertEquals("user", message.getRole());
        assertInstanceOf(BaiYingMessage.MessageContent.class, message.getContent());
        BaiYingMessage.MessageContent mc = (BaiYingMessage.MessageContent) message.getContent();
        assertEquals("hi", mc.getText());
        assertEquals(List.of(), mc.getFiles());
        assertEquals(List.of(), mc.getResources());
    }

    @Test
    void deserializeHandlesStringMessageContent() {
        Map<String, Object> wireMessage = new HashMap<>();
        wireMessage.put("role", "assistant");
        wireMessage.put("content", "plain text reply");

        Object result = codec.deserialize(List.of(wireMessage));

        BaiYingMessage message = (BaiYingMessage) result;
        assertEquals("assistant", message.getRole());
        assertEquals("plain text reply", message.getContent());
    }

    @Test
    void deserializeKeepsMultiMessageArrayAsListOfMessages() {
        Map<String, Object> first = new HashMap<>();
        first.put("role", "user");
        first.put("content", Map.of("text", "first"));
        Map<String, Object> second = new HashMap<>();
        second.put("role", "assistant");
        second.put("content", Map.of("text", "second"));

        Object result = codec.deserialize(List.of(first, second));

        assertInstanceOf(List.class, result);
        List<?> messages = (List<?>) result;
        assertEquals(2, messages.size());
        assertEquals("user", ((BaiYingMessage) messages.get(0)).getRole());
        assertEquals("assistant", ((BaiYingMessage) messages.get(1)).getRole());
    }

    @Test
    void serializePassesPlainStringThrough() {
        assertEquals("hello", codec.serialize("hello"));
    }

    @Test
    void serializeWrapsScalarMessageIntoSingleItemWireArray() {
        BaiYingMessage message = BaiYingMessage.builder()
                .role("user")
                .content(BaiYingMessage.MessageContent.builder().text("hi").build())
                .build();

        Object wire = codec.serialize(message);

        assertInstanceOf(List.class, wire);
        List<?> list = (List<?>) wire;
        assertEquals(1, list.size());
        @SuppressWarnings("unchecked")
        Map<String, Object> entry = (Map<String, Object>) list.get(0);
        assertEquals("user", entry.get("role"));
    }

    @Test
    void roundTripsAScalarMessage() {
        Map<String, Object> wireContent = new HashMap<>();
        wireContent.put("text", "round trip");
        Map<String, Object> wireMessage = new HashMap<>();
        wireMessage.put("role", "user");
        wireMessage.put("content", wireContent);

        Object decoded = codec.deserialize(List.of(wireMessage));
        Object reencoded = codec.serialize(decoded);
        Object redecoded = codec.deserialize(reencoded);

        assertInstanceOf(BaiYingMessage.class, redecoded);
        assertEquals("user", ((BaiYingMessage) redecoded).getRole());
    }
}
