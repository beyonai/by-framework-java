package com.iwhaleai.byai.framework.core.extensions;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Manager for agent configurations.
 * Provides CRUD operations, search, filtering, and capability-based lookups.
 */
@Slf4j
public class AgentConfigManager {

    private final Map<String, AgentConfig> configs = new ConcurrentHashMap<>();

    public AgentConfigManager() {
    }

    public AgentConfigManager(Collection<AgentConfig> initialConfigs) {
        addConfigs(initialConfigs);
    }

    /**
     * Add a single config.
     *
     * @return true if added, false if skipped due to conflict
     */
    public boolean addConfig(AgentConfig config) {
        if (config == null || config.getAgentId() == null) {
            return false;
        }

        String agentId = config.getAgentId();
        ConflictStrategy strategy = config.getOnConflict() != null
                ? config.getOnConflict()
                : ConflictStrategy.ERROR;

        configs.compute(agentId, (key, existing) -> {
            if (existing == null) {
                return config;
            }
            switch (strategy) {
                case OVERWRITE:
                    log.debug("Overwriting config for agent: {}", agentId);
                    return config;
                case SKIP:
                    log.debug("Skipping duplicate config for agent: {}", agentId);
                    return existing;
                case ERROR:
                default:
                    throw new IllegalStateException("Config already exists for agent: " + agentId);
            }
        });
        return true;
    }

    /**
     * Add multiple configs.
     *
     * @return list of boolean results for each config
     */
    public List<Boolean> addConfigs(Collection<AgentConfig> configs) {
        return configs.stream()
                .map(this::addConfig)
                .collect(Collectors.toList());
    }

    /**
     * Remove a config by agent ID.
     *
     * @return true if removed, false if not found
     */
    public boolean removeConfig(String agentId) {
        return configs.remove(agentId) != null;
    }

    /**
     * Remove all configs.
     */
    public void removeAllConfigs() {
        configs.clear();
    }

    /**
     * Get a config by agent ID.
     */
    public Optional<AgentConfig> getConfig(String agentId) {
        return Optional.ofNullable(configs.get(agentId));
    }

    /**
     * Get all configs.
     */
    public List<AgentConfig> listConfigs() {
        return new ArrayList<>(configs.values());
    }

    /**
     * Get all agent IDs.
     */
    public List<String> listAgentIds() {
        return new ArrayList<>(configs.keySet());
    }

    /**
     * Get the number of configs.
     */
    public int count() {
        return configs.size();
    }

    /**
     * Check if a config exists.
     */
    public boolean hasConfig(String agentId) {
        return configs.containsKey(agentId);
    }

    /**
     * Search configs by various criteria.
     * All parameters are optional (null means don't filter by that criterion).
     */
    public List<AgentConfig> searchConfigs(String name, String toolName, String callbackType, Boolean hasSubAgents) {
        Predicate<AgentConfig> predicate = config -> {
            if (name != null && !name.isEmpty()) {
                if (config.getName() == null || !config.getName().contains(name)) {
                    return false;
                }
            }
            if (toolName != null && !toolName.isEmpty()) {
                if (config.getTools() == null || !config.getTools().containsKey(toolName)) {
                    return false;
                }
            }
            if (callbackType != null && !callbackType.isEmpty()) {
                if (config.getCallbacks() == null || !config.getCallbacks().containsKey(callbackType)) {
                    return false;
                }
            }
            if (hasSubAgents != null) {
                boolean hasSubs = config.getSubAgents() != null && !config.getSubAgents().isEmpty();
                if (hasSubAgents != hasSubs) {
                    return false;
                }
            }
            return true;
        };

        return configs.values().stream()
                .filter(predicate)
                .collect(Collectors.toList());
    }

    /**
     * Find agents that have a specific tool.
     */
    public List<AgentConfig> getAgentByTool(String toolName) {
        return searchConfigs(null, toolName, null, null);
    }

    /**
     * Find agents that have a specific skill.
     */
    public List<AgentConfig> getAgentBySkill(String skillName) {
        return configs.values().stream()
                .filter(c -> c.getSkills() != null && c.getSkills().containsKey(skillName))
                .collect(Collectors.toList());
    }

    /**
     * Find agents that have a specific knowledge base.
     */
    public List<AgentConfig> getAgentByKnowledgeBase(String kbName) {
        return configs.values().stream()
                .filter(c -> c.getKnowledgeBases() != null && c.getKnowledgeBases().containsKey(kbName))
                .collect(Collectors.toList());
    }

    /**
     * Get sub-agents of a specific agent.
     */
    public List<AgentConfig> getSubAgents(String agentId) {
        AgentConfig config = configs.get(agentId);
        if (config == null || config.getSubAgents() == null) {
            return Collections.emptyList();
        }
        return config.getSubAgents().stream()
                .map(configs::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Update specific fields of a config.
     *
     * @return the updated config, or null if not found
     */
    public AgentConfig updateConfig(String agentId, Map<String, Object> updates) {
        AgentConfig existing = configs.get(agentId);
        if (existing == null) {
            return null;
        }

        AgentConfig updated = copyWithUpdates(existing, updates);
        configs.put(agentId, updated);
        return updated;
    }

    /**
     * Replace all configs with new set.
     */
    public void setConfigs(Collection<AgentConfig> newConfigs) {
        configs.clear();
        addConfigs(newConfigs);
    }

    /**
     * Export configs as a map (agentId -> config).
     */
    public Map<String, AgentConfig> toMap() {
        return new HashMap<>(configs);
    }

    private AgentConfig copyWithUpdates(AgentConfig original, Map<String, Object> updates) {
        AgentConfig.AgentConfigBuilder builder = AgentConfig.builder()
                .agentId(original.getAgentId())
                .name(original.getName())
                .description(original.getDescription())
                .prompts(original.getPrompts() != null ? new HashMap<>(original.getPrompts()) : new HashMap<>())
                .tools(original.getTools() != null ? new HashMap<>(original.getTools()) : new HashMap<>())
                .skills(original.getSkills() != null ? new HashMap<>(original.getSkills()) : new HashMap<>())
                .callbacks(original.getCallbacks() != null ? new HashMap<>(original.getCallbacks()) : new HashMap<>())
                .knowledgeBases(original.getKnowledgeBases() != null ? new HashMap<>(original.getKnowledgeBases()) : new HashMap<>())
                .subAgents(original.getSubAgents() != null ? new ArrayList<>(original.getSubAgents()) : new ArrayList<>())
                .extra(original.getExtra() != null ? new HashMap<>(original.getExtra()) : new HashMap<>())
                .onConflict(original.getOnConflict());

        // Apply updates
        if (updates.containsKey("name")) {
            builder.name((String) updates.get("name"));
        }
        if (updates.containsKey("description")) {
            builder.description((String) updates.get("description"));
        }
        if (updates.containsKey("prompts")) {
            builder.prompts((Map<String, Object>) updates.get("prompts"));
        }
        if (updates.containsKey("tools")) {
            builder.tools((Map<String, Object>) updates.get("tools"));
        }
        if (updates.containsKey("skills")) {
            builder.skills((Map<String, Object>) updates.get("skills"));
        }
        if (updates.containsKey("subAgents")) {
            builder.subAgents((List<String>) updates.get("subAgents"));
        }
        if (updates.containsKey("extra")) {
            builder.extra((Map<String, Object>) updates.get("extra"));
        }

        return builder.build();
    }
}
