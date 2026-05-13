package com.iwhaleai.byai.framework.core.protocol;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * Event for streaming content chunks.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StreamChunkEvent {
    private String content;
    @Builder.Default
    private String role = "assistant";
    private Map<String, Object> functionCall;
    private List<Map<String, Object>> toolCalls;
    @Builder.Default
    private Map<String, Object> metadata = Map.of();
}
