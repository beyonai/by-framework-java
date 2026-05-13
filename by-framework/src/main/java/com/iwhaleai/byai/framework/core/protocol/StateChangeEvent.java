package com.iwhaleai.byai.framework.core.protocol;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Event indicating a state change in agent execution.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StateChangeEvent {
    private String state;
    @Builder.Default
    private Map<String, Object> metadata = Map.of();
}
