package com.iwhaleai.byai.framework.common;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Constants - ConsumerGroups autoGroupName method.
 */
class ConstantsTest {

    @AfterEach
    void clearKeySchemaVersionProperty() {
        System.clearProperty("REDIS_KEY_SCHEMA_VERSION");
    }

    @Test
    void keySchemaVersionDefaultsToV1() {
        assertEquals("v1", Constants.getKeySchemaVersion());
    }

    @Test
    void keySchemaVersionAcceptsExplicitV2() {
        System.setProperty("REDIS_KEY_SCHEMA_VERSION", "v2");
        assertEquals("v2", Constants.getKeySchemaVersion());
    }

    @Test
    void keySchemaVersionRejectsInvalidValue() {
        System.setProperty("REDIS_KEY_SCHEMA_VERSION", "v3");
        assertThrows(IllegalArgumentException.class, Constants::getKeySchemaVersion);
    }

    @Test
    void autoGroupNameGeneratesConsistentHashForSameInput() {
        // Given
        List<String> agentTypes1 = List.of("agent-a", "agent-b", "agent-c");
        List<String> agentTypes2 = List.of("agent-c", "agent-b", "agent-a"); // Same elements, different order

        // When
        String group1 = Constants.ConsumerGroups.autoGroupName(agentTypes1);
        String group2 = Constants.ConsumerGroups.autoGroupName(agentTypes2);

        // Then - same hash since sorted input is identical
        assertEquals(group1, group2);
    }

    @Test
    void autoGroupNameGeneratesDifferentHashForDifferentInput() {
        // Given
        List<String> agentTypes1 = List.of("agent-a", "agent-b");
        List<String> agentTypes2 = List.of("agent-x", "agent-y");

        // When
        String group1 = Constants.ConsumerGroups.autoGroupName(agentTypes1);
        String group2 = Constants.ConsumerGroups.autoGroupName(agentTypes2);

        // Then
        assertNotEquals(group1, group2);
    }

    @Test
    void autoGroupNameStartsWithAgentEnginesPrefix() {
        // Given
        List<String> agentTypes = List.of("test-agent");

        // When
        String groupName = Constants.ConsumerGroups.autoGroupName(agentTypes);

        // Then
        assertTrue(groupName.startsWith(Constants.ConsumerGroups.AGENT_ENGINES));
    }

    @Test
    void autoGroupNameHandlesEmptyList() {
        // Given
        List<String> emptyAgentTypes = List.of();

        // When
        String groupName = Constants.ConsumerGroups.autoGroupName(emptyAgentTypes);

        // Then
        assertTrue(groupName.startsWith(Constants.ConsumerGroups.AGENT_ENGINES));
    }

    @Test
    void autoGroupNameHandlesSingleCapability() {
        // Given
        List<String> singleCap = List.of("solo-agent");

        // When
        String groupName = Constants.ConsumerGroups.autoGroupName(singleCap);

        // Then
        assertTrue(groupName.startsWith(Constants.ConsumerGroups.AGENT_ENGINES));
        assertTrue(groupName.contains(":"));
    }

    @Test
    void terminalStatesContainsExpectedValues() {
        assertTrue(Constants.TERMINAL_STATES.contains("COMPLETED"));
        assertTrue(Constants.TERMINAL_STATES.contains("FAILED"));
        assertTrue(Constants.TERMINAL_STATES.contains("CANCELLED"));
        assertTrue(Constants.TERMINAL_STATES.contains("SUCCESS"));
    }

    @Test
    void terminalStatesDoesNotContainNonTerminalValues() {
        assertFalse(Constants.TERMINAL_STATES.contains("RUNNING"));
        assertFalse(Constants.TERMINAL_STATES.contains("PENDING"));
    }

    @Test
    void queueNamesGenerateCorrectPatterns() {
        // Given
        String sessionId = "sess-123";

        // When
        String dataStream = Constants.QueueNames.sessionDataStream(sessionId);
        String ctrlStream = Constants.QueueNames.ctrlStream("my-agent");
        String workerCtrlStream = Constants.QueueNames.workerCtrlStream("worker-1");

        // Then
        assertTrue(dataStream.contains(sessionId));
        assertTrue(dataStream.contains("session"));
        assertTrue(dataStream.contains("data_stream"));
        assertTrue(ctrlStream.contains("ctrl"));
        assertTrue(ctrlStream.contains("agent_type"));
        assertTrue(workerCtrlStream.contains("ctrl"));
        assertTrue(workerCtrlStream.contains("worker"));
    }

    @Test
    void registryKeysFollowExpectedPatterns() {
        // When/Then
        assertTrue(Constants.RegistryKeys.KNOWN_WORKERS.contains("workers"));
        assertTrue(Constants.RegistryKeys.workerDeclaredAgentTypes("w1").contains("agent_types"));
        assertTrue(Constants.RegistryKeys.agentTypeMembers("cap1").contains("agent_type"));
        assertTrue(Constants.RegistryKeys.workerOnlineLease("w1").contains("online"));
        assertTrue(Constants.RegistryKeys.taskGroup("g1").contains("task_group"));
    }
}
