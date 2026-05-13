package com.iwhaleai.byai.framework.core.protocol;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Event for requesting user input.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AskUserEvent {
    private String prompt;
    @Builder.Default
    private Map<String, Object> metadata = Map.of();
}
