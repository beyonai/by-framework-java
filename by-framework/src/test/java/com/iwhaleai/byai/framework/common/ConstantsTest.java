package com.iwhaleai.byai.framework.common;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.util.JedisClusterCRC16;

import java.util.Arrays;
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
        assertTrue(Constants.RegistryKeys.knownWorkers().contains("workers"));
        assertTrue(Constants.RegistryKeys.workerDeclaredAgentTypes("w1").contains("agent_types"));
        assertTrue(Constants.RegistryKeys.agentTypeMembers("cap1").contains("agent_type"));
        assertTrue(Constants.RegistryKeys.workerOnlineLease("w1").contains("online"));
        assertTrue(Constants.RegistryKeys.taskGroup("g1").contains("task_group"));
    }

    // --- Phase 2a: hash-tag key matrix + trace namespace unification (issue #9) ---

    private void setSchemaVersion(String version) {
        System.setProperty("REDIS_KEY_SCHEMA_VERSION", version);
    }

    @Test
    void goldenV1KeysAreByteForByteUnchanged() {
        setSchemaVersion("v1");

        assertEquals("byai_gateway:ctrl:agent_type:chat", Constants.QueueNames.ctrlStream("chat"));
        assertEquals("byai_gateway:ctrl:worker:worker-01", Constants.QueueNames.workerCtrlStream("worker-01"));
        assertEquals(
                "byai_gateway:session:sess-abc123:data_stream",
                Constants.QueueNames.sessionDataStream("sess-abc123"));
        assertEquals(
                "byai_gateway:control_plane:mgmt:wakeup", Constants.QueueNames.controlPlaneManagementStream());
        assertEquals(
                "byai_gateway:control_plane:mgmt:wakeup:result:exec-1",
                Constants.QueueNames.controlPlaneDecisionStream("exec-1"));
        assertEquals(
                "byai_gateway:control_plane:mgmt:delivery:pending",
                Constants.QueueNames.controlPlanePendingQueue());
        assertEquals(
                "byai_gateway:control_plane:mgmt:deadletter", Constants.QueueNames.controlPlaneDeadletterStream());
        assertEquals(
                "byai_gateway:control_plane:circuit:agent_type:chat",
                Constants.QueueNames.controlPlaneCircuitBreakerKey("chat"));
        assertEquals(
                "byai_gateway:control_plane:quota:user:user-1",
                Constants.QueueNames.controlPlaneQuotaKey("user-1"));
        assertEquals(
                "byai_gateway:control_plane:availability:agent_type:chat",
                Constants.QueueNames.controlPlaneAgentAvailability("chat"));
        assertEquals(
                "byai_gateway:control_plane:fallback:agent_type:chat",
                Constants.QueueNames.controlPlaneAgentFallback("chat"));
        assertEquals(
                "byai_gateway:control_plane:wakeup:dedupe:chat:user-1:us-east",
                Constants.QueueNames.controlPlaneWakeupDedupe("chat", "user-1", "us-east"));

        assertEquals("byai_gateway:registry:workers", Constants.RegistryKeys.knownWorkers());
        assertEquals(
                "byai_gateway:registry:worker:agent_types:worker-01",
                Constants.RegistryKeys.workerDeclaredAgentTypes("worker-01"));
        assertEquals(
                "byai_gateway:registry:agent_type:workers:chat", Constants.RegistryKeys.agentTypeMembers("chat"));
        assertEquals(
                "byai_gateway:registry:worker:lock:worker-01", Constants.RegistryKeys.workerLock("worker-01"));
        assertEquals(
                "byai_gateway:registry:worker:online:worker-01",
                Constants.RegistryKeys.workerOnlineLease("worker-01"));
        assertEquals(
                "byai_gateway:session:sess-abc123:registry", Constants.RegistryKeys.sessionRegistry("sess-abc123"));
        assertEquals("byai_gateway:task_group:tg-1", Constants.RegistryKeys.taskGroup("tg-1"));
        assertEquals("byai_gateway:task_group:tg-1:results", Constants.RegistryKeys.taskGroupResults("tg-1"));
        assertEquals("byai_gateway:sd:services", Constants.RegistryKeys.sdServices());
        assertEquals("byai_gateway:sd:active:svc-a", Constants.RegistryKeys.sdActiveInstances("svc-a"));
        assertEquals("byai_gateway:sd:instances:svc-a", Constants.RegistryKeys.sdInstanceDetails("svc-a"));
        assertEquals(
                "byai_gateway:registry:worker:admin:worker-01", Constants.RegistryKeys.workerAdminState("worker-01"));
        assertEquals("byai_gateway:registry:agent_type:denied:chat", Constants.RegistryKeys.agentTypeDenied("chat"));

        assertEquals("by_framework:trace:trace-xyz", Constants.TraceKeys.traceMeta("trace-xyz"));
        assertEquals("by_framework:trace:spans:trace-xyz", Constants.TraceKeys.traceSpans("trace-xyz"));
        assertEquals(
                "by_framework:trace:idx:session:sess-abc123", Constants.TraceKeys.traceIndexSession("sess-abc123"));
        assertEquals("by_framework:trace:idx:worker:worker-01", Constants.TraceKeys.traceIndexWorker("worker-01"));
        assertEquals("by_framework:trace:idx:agent:chat", Constants.TraceKeys.traceIndexAgent("chat"));
    }

    @Test
    void goldenV2Keys() {
        setSchemaVersion("v2");

        assertEquals("byai_gateway:v2:ctrl:agent_type:chat", Constants.QueueNames.ctrlStream("chat"));
        assertEquals(
                "byai_gateway:v2:ctrl:worker:{worker-01}", Constants.QueueNames.workerCtrlStream("worker-01"));
        assertEquals(
                "byai_gateway:v2:session:{sess-abc123}:data_stream",
                Constants.QueueNames.sessionDataStream("sess-abc123"));
        assertEquals(
                "byai_gateway:v2:control_plane:mgmt:wakeup", Constants.QueueNames.controlPlaneManagementStream());
        assertEquals(
                "byai_gateway:v2:control_plane:mgmt:wakeup:result:exec-1",
                Constants.QueueNames.controlPlaneDecisionStream("exec-1"));
        assertEquals(
                "byai_gateway:v2:control_plane:mgmt:delivery:pending",
                Constants.QueueNames.controlPlanePendingQueue());
        assertEquals(
                "byai_gateway:v2:control_plane:mgmt:deadletter",
                Constants.QueueNames.controlPlaneDeadletterStream());
        assertEquals(
                "byai_gateway:v2:control_plane:circuit:agent_type:chat",
                Constants.QueueNames.controlPlaneCircuitBreakerKey("chat"));
        assertEquals(
                "byai_gateway:v2:control_plane:quota:user:user-1",
                Constants.QueueNames.controlPlaneQuotaKey("user-1"));
        assertEquals(
                "byai_gateway:v2:control_plane:availability:agent_type:chat",
                Constants.QueueNames.controlPlaneAgentAvailability("chat"));
        assertEquals(
                "byai_gateway:v2:control_plane:fallback:agent_type:chat",
                Constants.QueueNames.controlPlaneAgentFallback("chat"));
        assertEquals(
                "byai_gateway:v2:control_plane:wakeup:dedupe:chat:user-1:us-east",
                Constants.QueueNames.controlPlaneWakeupDedupe("chat", "user-1", "us-east"));

        assertEquals("byai_gateway:v2:registry:workers", Constants.RegistryKeys.knownWorkers());
        assertEquals(
                "byai_gateway:v2:registry:worker:{worker-01}:agent_types",
                Constants.RegistryKeys.workerDeclaredAgentTypes("worker-01"));
        assertEquals(
                "byai_gateway:v2:registry:agent_type:{chat}:workers",
                Constants.RegistryKeys.agentTypeMembers("chat"));
        assertEquals(
                "byai_gateway:v2:registry:worker:{worker-01}:lock", Constants.RegistryKeys.workerLock("worker-01"));
        assertEquals(
                "byai_gateway:v2:registry:worker:{worker-01}:online",
                Constants.RegistryKeys.workerOnlineLease("worker-01"));
        assertEquals(
                "byai_gateway:v2:session:{sess-abc123}:registry",
                Constants.RegistryKeys.sessionRegistry("sess-abc123"));
        assertEquals("byai_gateway:v2:task_group:{tg-1}", Constants.RegistryKeys.taskGroup("tg-1"));
        assertEquals(
                "byai_gateway:v2:task_group:{tg-1}:results", Constants.RegistryKeys.taskGroupResults("tg-1"));
        assertEquals("byai_gateway:v2:sd:services", Constants.RegistryKeys.sdServices());
        assertEquals(
                "byai_gateway:v2:sd:{svc-a}:active", Constants.RegistryKeys.sdActiveInstances("svc-a"));
        assertEquals(
                "byai_gateway:v2:sd:{svc-a}:instances", Constants.RegistryKeys.sdInstanceDetails("svc-a"));
        assertEquals(
                "byai_gateway:v2:registry:worker:{worker-01}:admin",
                Constants.RegistryKeys.workerAdminState("worker-01"));
        assertEquals(
                "byai_gateway:v2:registry:agent_type:{chat}:denied", Constants.RegistryKeys.agentTypeDenied("chat"));

        assertEquals("byai_gateway:v2:trace:{trace-xyz}", Constants.TraceKeys.traceMeta("trace-xyz"));
        assertEquals("byai_gateway:v2:trace:spans:{trace-xyz}", Constants.TraceKeys.traceSpans("trace-xyz"));
        assertEquals(
                "byai_gateway:v2:trace:idx:session:sess-abc123",
                Constants.TraceKeys.traceIndexSession("sess-abc123"));
        assertEquals(
                "byai_gateway:v2:trace:idx:worker:worker-01", Constants.TraceKeys.traceIndexWorker("worker-01"));
        assertEquals("byai_gateway:v2:trace:idx:agent:chat", Constants.TraceKeys.traceIndexAgent("chat"));
    }

    private void assertSameSlot(String... keys) {
        int slot = JedisClusterCRC16.getSlot(keys[0]);
        for (String key : keys) {
            assertEquals(
                    slot, JedisClusterCRC16.getSlot(key), "keys landed on different slots: " + Arrays.toString(keys));
        }
    }

    @Test
    void sessionGroupSharesASlotUnderV2() {
        setSchemaVersion("v2");
        assertSameSlot(
                Constants.QueueNames.sessionDataStream("sess-abc123"),
                Constants.RegistryKeys.sessionRegistry("sess-abc123"));
    }

    @Test
    void workerGroupSharesASlotUnderV2() {
        setSchemaVersion("v2");
        assertSameSlot(
                Constants.QueueNames.workerCtrlStream("worker-01"),
                Constants.RegistryKeys.workerDeclaredAgentTypes("worker-01"),
                Constants.RegistryKeys.workerLock("worker-01"),
                Constants.RegistryKeys.workerOnlineLease("worker-01"),
                Constants.RegistryKeys.workerAdminState("worker-01"));
    }

    @Test
    void taskGroupSharesASlotUnderV2() {
        setSchemaVersion("v2");
        assertSameSlot(
                Constants.RegistryKeys.taskGroup("tg-1"), Constants.RegistryKeys.taskGroupResults("tg-1"));
    }

    @Test
    void agentTypeGroupSharesASlotUnderV2() {
        // Mandatory: WorkerRegistry.denyWorkerForType() writes agentTypeDenied and
        // agentTypeMembers together and must keep working atomically under Cluster.
        setSchemaVersion("v2");
        assertSameSlot(
                Constants.RegistryKeys.agentTypeMembers("chat"), Constants.RegistryKeys.agentTypeDenied("chat"));
    }

    @Test
    void serviceDiscoveryGroupSharesASlotUnderV2() {
        setSchemaVersion("v2");
        assertSameSlot(
                Constants.RegistryKeys.sdActiveInstances("svc-a"), Constants.RegistryKeys.sdInstanceDetails("svc-a"));
    }

    @Test
    void traceGroupSharesASlotUnderV2() {
        setSchemaVersion("v2");
        assertSameSlot(Constants.TraceKeys.traceMeta("trace-xyz"), Constants.TraceKeys.traceSpans("trace-xyz"));
    }

    @Test
    void workerOnlineLeaseScanPatternRoundTripsUnderV1() {
        setSchemaVersion("v1");
        String realKey = Constants.RegistryKeys.workerOnlineLease("worker-01");
        assertEquals("byai_gateway:registry:worker:online:*", Constants.RegistryKeys.workerOnlineLeaseScanPattern());
        assertEquals("worker-01", Constants.RegistryKeys.workerIdFromOnlineLeaseKey(realKey));
    }

    @Test
    void workerOnlineLeaseScanPatternRoundTripsUnderV2() {
        setSchemaVersion("v2");
        String realKey = Constants.RegistryKeys.workerOnlineLease("worker-01");
        assertEquals(
                "byai_gateway:v2:registry:worker:{*}:online",
                Constants.RegistryKeys.workerOnlineLeaseScanPattern());
        assertEquals("worker-01", Constants.RegistryKeys.workerIdFromOnlineLeaseKey(realKey));
    }
}
