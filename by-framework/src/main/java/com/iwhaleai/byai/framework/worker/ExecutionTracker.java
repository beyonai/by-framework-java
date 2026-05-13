package com.iwhaleai.byai.framework.worker;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks running executions with dual indexing by executionId and messageId.
 * Similar to Python's ExecutionTracker class.
 */
@Slf4j
public class ExecutionTracker {

    private final Map<String, RunningExecution> activeExecutions = new ConcurrentHashMap<>();
    private final Map<String, String> messageToExecution = new ConcurrentHashMap<>();
    private final AtomicInteger activeCount = new AtomicInteger(0);

    /**
     * Add a running execution to the tracker.
     */
    public void addExecution(RunningExecution execution) {
        activeExecutions.put(execution.getExecutionId(), execution);
        if (execution.getMessageId() != null) {
            messageToExecution.put(execution.getMessageId(), execution.getExecutionId());
        }
        activeCount.incrementAndGet();
        log.debug("Execution added: {}", execution.getExecutionId());
    }

    /**
     * Get an execution by execution ID.
     */
    public RunningExecution getExecution(String executionId) {
        return activeExecutions.get(executionId);
    }

    /**
     * Get an execution by message ID.
     */
    public RunningExecution getExecutionByMessage(String messageId) {
        String executionId = messageToExecution.get(messageId);
        if (executionId != null) {
            return activeExecutions.get(executionId);
        }
        return null;
    }

    /**
     * Remove an execution by execution ID.
     *
     * @return the removed execution, or null if not found
     */
    public RunningExecution removeExecution(String executionId) {
        RunningExecution execution = activeExecutions.remove(executionId);
        if (execution != null && execution.getMessageId() != null) {
            messageToExecution.remove(execution.getMessageId());
        }
        if (execution != null) {
            activeCount.decrementAndGet();
            log.debug("Execution removed: {}", executionId);
        }
        return execution;
    }

    /**
     * Remove an execution by message ID.
     */
    public RunningExecution removeByMessage(String messageId) {
        String executionId = messageToExecution.remove(messageId);
        if (executionId != null) {
            return removeExecution(executionId);
        }
        return null;
    }

    /**
     * Get the number of active executions.
     */
    public int getActiveCount() {
        return activeCount.get();
    }

    /**
     * Check if there are active executions.
     */
    public boolean hasActiveExecutions() {
        return activeCount.get() > 0;
    }

    /**
     * Get all active execution IDs.
     */
    public Map<String, RunningExecution> getActiveExecutions() {
        return Map.copyOf(activeExecutions);
    }
}
