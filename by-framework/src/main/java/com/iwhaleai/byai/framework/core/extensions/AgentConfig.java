package com.iwhaleai.byai.framework.core.extensions;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AgentConfig {
    private String agentId;
    @Builder.Default
    private String name = "";
    @Builder.Default
    private String description = "";
    @Builder.Default
    private Map<String, Object> prompts = new HashMap<>();
    @Builder.Default
    private Map<String, Object> tools = new HashMap<>();
    @Builder.Default
    private Map<String, Object> skills = new HashMap<>();
    @Builder.Default
    private Map<CallbackType, List<Runnable>> callbacks = new HashMap<>();
    @Builder.Default
    private Map<String, Object> knowledgeBases = new HashMap<>();
    @Builder.Default
    private List<String> subAgents = new ArrayList<>();
    @Builder.Default
    private Map<String, Object> extra = new HashMap<>();
    @Builder.Default
    private ConflictStrategy onConflict = ConflictStrategy.ERROR;
}
