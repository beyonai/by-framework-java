package com.iwhaleai.byai.framework.core.protocol;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AgentTaskResultTest {

    @Test
    void normalizeStructuredResultPreservesFields() {
        AgentTaskResult result = AgentTaskResult.normalize(new AgentTaskResult(
                AgentState.COMPLETED,
                "done",
                Map.of("answer", 42),
                Map.of("tokens", 123),
                Map.of("debug_id", "abc")
        ));

        assertEquals(AgentState.COMPLETED, result.status());
        assertEquals("done", result.content());
        assertEquals(Map.of("answer", 42), result.replyData());
        assertEquals(Map.of("tokens", 123), result.metadata());
        assertEquals(Map.of("debug_id", "abc"), result.extraPayload());
    }

    @Test
    void normalizeLegacyMapCopiesMetadataWithoutRemovingReplyData() {
        Map<String, Object> legacyResult = Map.of(
                "status", AgentState.COMPLETED,
                "answer", "42",
                "metadata", Map.of("tokens", 123)
        );

        AgentTaskResult result = AgentTaskResult.normalize(legacyResult);

        assertEquals(AgentState.COMPLETED, result.status());
        assertEquals(legacyResult, result.replyData());
        assertEquals(Map.of("tokens", 123), result.metadata());
    }

    @Test
    void normalizeStructuredMapUsesReplyDataField() {
        AgentTaskResult result = AgentTaskResult.normalize(Map.of(
                "status", AgentState.COMPLETED,
                "content", "done",
                "reply_data", Map.of("answer", "42"),
                "metadata", Map.of("tokens", 123),
                "extra_payload", Map.of("debug_id", "abc")
        ));

        assertEquals(Map.of("answer", "42"), result.replyData());
        assertEquals(Map.of("tokens", 123), result.metadata());
        assertEquals(Map.of("debug_id", "abc"), result.extraPayload());
    }

    @Test
    void normalizeRejectsNonJsonSerializableReplyData() {
        Object custom = new Object();

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> AgentTaskResult.normalize(Map.of("item", custom))
        );
        assertEquals(
                "processCommand return value must be JSON serializable; got Object at replyData.item",
                error.getMessage()
        );
    }
}
