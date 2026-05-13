package com.iwhaleai.byai.framework.common.exceptions;

/**
 * Exception thrown when no worker is found for an agent type.
 */
public class WorkerNotFoundException extends FrameworkException {

    private final String agentType;

    public WorkerNotFoundException(String agentType) {
        super("No worker found for agent type: " + agentType);
        this.agentType = agentType;
    }

    public String getAgentType() {
        return agentType;
    }
}
