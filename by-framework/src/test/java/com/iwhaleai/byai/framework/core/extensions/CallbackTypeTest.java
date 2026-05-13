package com.iwhaleai.byai.framework.core.extensions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for CallbackType enum.
 */
class CallbackTypeTest {

    @Test
    void callbackTypesHaveCorrectValues() {
        assertEquals("before_model_callback", CallbackType.BEFORE_MODEL_CALLBACK.getValue());
        assertEquals("after_model_callback", CallbackType.AFTER_MODEL_CALLBACK.getValue());
        assertEquals("before_tool_callback", CallbackType.BEFORE_TOOL_CALLBACK.getValue());
        assertEquals("after_tool_callback", CallbackType.AFTER_TOOL_CALLBACK.getValue());
        assertEquals("before_agent_callback", CallbackType.BEFORE_AGENT_CALLBACK.getValue());
        assertEquals("after_agent_callback", CallbackType.AFTER_AGENT_CALLBACK.getValue());
    }

    @Test
    void callbackTypesCount() {
        assertEquals(6, CallbackType.values().length);
    }

    @Test
    void callbackTypesCanBeFoundByName() {
        assertEquals(CallbackType.BEFORE_MODEL_CALLBACK, CallbackType.valueOf("BEFORE_MODEL_CALLBACK"));
        assertEquals(CallbackType.AFTER_MODEL_CALLBACK, CallbackType.valueOf("AFTER_MODEL_CALLBACK"));
        assertEquals(CallbackType.BEFORE_TOOL_CALLBACK, CallbackType.valueOf("BEFORE_TOOL_CALLBACK"));
        assertEquals(CallbackType.AFTER_TOOL_CALLBACK, CallbackType.valueOf("AFTER_TOOL_CALLBACK"));
        assertEquals(CallbackType.BEFORE_AGENT_CALLBACK, CallbackType.valueOf("BEFORE_AGENT_CALLBACK"));
        assertEquals(CallbackType.AFTER_AGENT_CALLBACK, CallbackType.valueOf("AFTER_AGENT_CALLBACK"));
    }
}