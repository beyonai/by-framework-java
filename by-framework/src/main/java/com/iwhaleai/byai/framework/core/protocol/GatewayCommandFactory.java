package com.iwhaleai.byai.framework.core.protocol;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class GatewayCommandFactory {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Map<String, Class<? extends GatewayCommand>> COMMAND_REGISTRY = new ConcurrentHashMap<>();

    private GatewayCommandFactory() {
    }

    public static GatewayCommand fromJson(String data) throws Exception {
        return fromMap(OBJECT_MAPPER.readValue(data, Map.class));
    }

    public static GatewayCommand fromMap(Map<String, Object> data) {
        String actionType = String.valueOf(data.get("action_type"));
        Class<? extends GatewayCommand> commandClass = COMMAND_REGISTRY.get(actionType);
        if (commandClass == null) {
            throw new IllegalArgumentException("Unsupported action_type: " + actionType);
        }
        return OBJECT_MAPPER.convertValue(data, commandClass);
    }

    public static void registerCommand(String actionType, Class<? extends GatewayCommand> commandClass) {
        if (actionType == null || actionType.isBlank()) {
            throw new IllegalArgumentException("actionType is required");
        }
        COMMAND_REGISTRY.put(actionType, commandClass);
    }

    public static void unregisterCommand(String actionType) {
        COMMAND_REGISTRY.remove(actionType);
    }

    public static Class<? extends GatewayCommand> getRegisteredCommand(String actionType) {
        return COMMAND_REGISTRY.get(actionType);
    }

    static {
        registerCommand(ActionType.ASK_AGENT, AskAgentCommand.class);
        registerCommand(ActionType.RESUME, ResumeCommand.class);
        registerCommand(ActionType.CANCEL_TASK, CancelTaskCommand.class);
    }
}
