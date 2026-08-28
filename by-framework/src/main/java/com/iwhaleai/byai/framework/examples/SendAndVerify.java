package com.iwhaleai.byai.framework.examples;

import com.iwhaleai.byai.framework.client.GatewayClient;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.protocol.ActionType;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.List;
import java.util.Map;

/**
 * Deploy smoke test driver: sends a message to DemoWorker's "java-agent-demo" agent
 * type through a real Redis instance (deploy/docker-compose.yml) and verifies the
 * echoed reply actually arrives on the session's data stream — the only place a
 * message flows through a real, separate-process Redis Streams deployment instead
 * of the Mockito-mocked Jedis the test suite uses. See
 * .github/workflows/deploy-smoke-test.yml.
 */
public final class SendAndVerify {

    private static final String SESSION_ID = "smoke-test-session";
    private static final String EXPECTED_FRAGMENT = "Hello from Java SDK!";
    private static final long TIMEOUT_MS = 15_000;

    private SendAndVerify() {
    }

    public static void main(String[] args) {
        RedisClient redisClient = RedisClient.getInstance();
        GatewayClient<Object> client = new GatewayClient<>(redisClient);

        System.out.println("Sending message to java-agent-demo...");
        GatewayClient.SendResponse response = client.sendMessage(
                "java-agent-demo",
                SESSION_ID,
                "smoke test ping",
                "smoke-user",
                "smoke-user",
                ActionType.ASK_AGENT,
                null,
                null,
                null,
                Map.of(),
                Map.of());

        if (!response.isSuccess()) {
            System.err.println("Send failed: " + response.getError());
            System.exit(1);
        }

        if (!waitForEchoedReply(redisClient)) {
            System.err.println("Timed out waiting for echoed reply containing: " + EXPECTED_FRAGMENT);
            System.exit(1);
        }

        System.out.println("Smoke test passed: worker echoed the expected reply.");
    }

    private static boolean waitForEchoedReply(RedisClient redisClient) {
        String streamName = Constants.QueueNames.sessionDataStream(SESSION_ID);
        long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        StreamEntryID lastId = new StreamEntryID("0-0");

        try (Jedis jedis = redisClient.getResource()) {
            while (System.currentTimeMillis() < deadline) {
                List<Map.Entry<String, List<StreamEntry>>> result = jedis.xread(
                        XReadParams.xReadParams().count(10).block(1000),
                        Map.of(streamName, lastId));
                if (result == null) {
                    continue;
                }
                for (Map.Entry<String, List<StreamEntry>> entry : result) {
                    for (StreamEntry streamEntry : entry.getValue()) {
                        lastId = streamEntry.getID();
                        String data = streamEntry.getFields().get("data");
                        if (data != null && data.contains(EXPECTED_FRAGMENT)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }
}
