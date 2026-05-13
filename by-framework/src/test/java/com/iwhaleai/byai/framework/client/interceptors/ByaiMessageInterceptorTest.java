package com.iwhaleai.byai.framework.client.interceptors;

import com.iwhaleai.byai.framework.core.protocol.BaiYingMessage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ByaiMessageInterceptor 测试，对标 Python 中 interceptor 逻辑
 */
class ByaiMessageInterceptorTest {

    private final ByaiMessageInterceptor interceptor = new ByaiMessageInterceptor();

    @Test
    void stringContentPassesThrough() {
        SendMessageParams params = SendMessageParams.builder()
                .targetAgentType("agent-x")
                .sessionId("sess-1")
                .content("hello world")
                .build();

        SendMessageParams result = interceptor.beforeSend(params);

        assertEquals("hello world", result.getContent());
    }

    @Test
    void nullContentPassesThrough() {
        SendMessageParams params = SendMessageParams.builder()
                .targetAgentType("agent-x")
                .sessionId("sess-1")
                .content(null)
                .build();

        SendMessageParams result = interceptor.beforeSend(params);

        assertNull(result.getContent());
    }

    @Test
    void singleBaiYingMessageConvertedToListOfMaps() {
        BaiYingMessage message = BaiYingMessage.builder()
                .role(BaiYingMessage.Role.USER.getValue())
                .content("hello from user")
                .build();

        SendMessageParams params = SendMessageParams.builder()
                .targetAgentType("agent-x")
                .sessionId("sess-1")
                .content(message)
                .build();

        SendMessageParams result = interceptor.beforeSend(params);

        assertInstanceOf(List.class, result.getContent());
        List<Map<String, Object>> content = (List<Map<String, Object>>) result.getContent();
        assertEquals(1, content.size());
        assertEquals("user", content.get(0).get("role"));
        assertEquals("hello from user", content.get(0).get("content"));
    }

    @Test
    void listOfBaiYingMessagesConvertedToListOfMaps() {
        List<BaiYingMessage> messages = List.of(
                BaiYingMessage.builder().role("user").content("question").build(),
                BaiYingMessage.builder().role("assistant").content("answer").build()
        );

        SendMessageParams params = SendMessageParams.builder()
                .targetAgentType("agent-x")
                .sessionId("sess-1")
                .content(messages)
                .build();

        SendMessageParams result = interceptor.beforeSend(params);

        List<Map<String, Object>> content = (List<Map<String, Object>>) result.getContent();
        assertEquals(2, content.size());
        assertEquals("user", content.get(0).get("role"));
        assertEquals("question", content.get(0).get("content"));
        assertEquals("assistant", content.get(1).get("role"));
        assertEquals("answer", content.get(1).get("content"));
    }

    @Test
    void baiYingMessageWithStructuredContentConvertsFiles() {
        BaiYingMessage.MessageFile file = BaiYingMessage.MessageFile.builder()
                .fileId("f1")
                .fileUrl("http://example.com/doc.pdf")
                .fileType("pdf")
                .fileName("doc.pdf")
                .fileSize(2048L)
                .build();

        BaiYingMessage.MessageContent msgContent = BaiYingMessage.MessageContent.builder()
                .text("check this file")
                .files(List.of(file))
                .build();

        BaiYingMessage message = BaiYingMessage.builder()
                .role("user")
                .content(msgContent)
                .build();

        SendMessageParams params = SendMessageParams.builder()
                .targetAgentType("agent-x")
                .sessionId("sess-1")
                .content(message)
                .build();

        SendMessageParams result = interceptor.beforeSend(params);

        List<Map<String, Object>> content = (List<Map<String, Object>>) result.getContent();
        assertEquals(1, content.size());

        Map<String, Object> converted = content.get(0);
        assertEquals("user", converted.get("role"));

        Map<String, Object> contentMap = (Map<String, Object>) converted.get("content");
        assertEquals("check this file", contentMap.get("text"));

        List<Map<String, Object>> files = (List<Map<String, Object>>) contentMap.get("files");
        assertEquals(1, files.size());
        assertEquals("f1", files.get(0).get("fileId"));
        assertEquals("http://example.com/doc.pdf", files.get(0).get("fileUrl"));
        assertEquals("pdf", files.get(0).get("fileType"));
        assertEquals("doc.pdf", files.get(0).get("fileName"));
        assertEquals(2048L, files.get(0).get("fileSize"));

        // Resources placeholder should be present
        assertNotNull(contentMap.get("resources"));
    }

    @Test
    void mapContentPassesThroughAsIs() {
        Map<String, Object> mapContent = Map.of("role", "user", "content", "raw map");

        SendMessageParams params = SendMessageParams.builder()
                .targetAgentType("agent-x")
                .sessionId("sess-1")
                .content(List.of(mapContent))
                .build();

        SendMessageParams result = interceptor.beforeSend(params);

        List<Map<String, Object>> content = (List<Map<String, Object>>) result.getContent();
        assertEquals(1, content.size());
        assertEquals("user", content.get(0).get("role"));
        assertEquals("raw map", content.get(0).get("content"));
    }

    @Test
    void unknownObjectFallsBackToToString() {
        Object unknownObj = 12345;

        SendMessageParams params = SendMessageParams.builder()
                .targetAgentType("agent-x")
                .sessionId("sess-1")
                .content(unknownObj)
                .build();

        SendMessageParams result = interceptor.beforeSend(params);

        List<Map<String, Object>> content = (List<Map<String, Object>>) result.getContent();
        assertEquals(1, content.size());
        assertEquals("12345", content.get(0).get("content"));
    }

    @Test
    void messageWithNullFilesProducesEmptyFilesList() {
        BaiYingMessage.MessageContent msgContent = BaiYingMessage.MessageContent.builder()
                .text("no files")
                .files(null)
                .build();

        BaiYingMessage message = BaiYingMessage.builder()
                .role("user")
                .content(msgContent)
                .build();

        SendMessageParams params = SendMessageParams.builder()
                .targetAgentType("agent-x")
                .sessionId("sess-1")
                .content(message)
                .build();

        SendMessageParams result = interceptor.beforeSend(params);

        List<Map<String, Object>> content = (List<Map<String, Object>>) result.getContent();
        Map<String, Object> contentMap = (Map<String, Object>) content.get(0).get("content");
        List<?> files = (List<?>) contentMap.get("files");
        assertTrue(files.isEmpty());
    }

    @Test
    void otherParamsFieldsArePreserved() {
        SendMessageParams params = SendMessageParams.builder()
                .targetAgentType("agent-x")
                .sessionId("sess-1")
                .content("test")
                .userCode("tenant-1")
                .userName("user-1")
                .actionType("ASK_AGENT")
                .parentMessageId("parent-1")
                .metadata(Map.of("key", "value"))
                .build();

        SendMessageParams result = interceptor.beforeSend(params);

        assertEquals("agent-x", result.getTargetAgentType());
        assertEquals("sess-1", result.getSessionId());
        assertEquals("tenant-1", result.getUserCode());
        assertEquals("user-1", result.getUserName());
        assertEquals("ASK_AGENT", result.getActionType());
        assertEquals("parent-1", result.getParentMessageId());
        assertEquals("value", result.getMetadata().get("key"));
    }
}
