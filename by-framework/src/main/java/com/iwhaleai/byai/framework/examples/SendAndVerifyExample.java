package com.iwhaleai.byai.framework.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iwhaleai.byai.framework.client.GatewayClient;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;

import java.util.List;

/**
 * Deploy smoke test driver: sends a message to {@link DemoWorker}'s
 * "java-agent-demo" agent type through a real Redis instance
 * (deploy/docker-compose.yml) and verifies the reply actually arrives on the
 * session's data stream - the only place a message flows through a real,
 * separate-process Redis Streams deployment instead of the in-process fakes
 * the test suite uses. Mirrors by-framework-ts's examples/send_and_verify.ts.
 * See .github/workflows/deploy-smoke-test.yml.
 */
public final class SendAndVerifyExample {

    private static final String SESSION_ID = "smoke-test-session-java";
    private static final String AGENT_TYPE = "java-agent-demo";
    private static final String EXPECTED_FRAGMENT = "Hello from Java SDK!";
    private static final long TIMEOUT_MS = 15_000L;
    private static final long POLL_INTERVAL_MS = 500L;
    private static final int READ_COUNT = 200;

    private SendAndVerifyExample() {
    }

    private static String reconstructReplyText(RedisClient redisClient, ObjectMapper mapper, String streamName) {
        List<StreamEntry> entries;
        try (Jedis jedis = redisClient.getResource()) {
            entries = jedis.xrevrange(streamName, StreamEntryID.MAXIMUM_ID, StreamEntryID.MINIMUM_ID, READ_COUNT);
        }

        // xrevrange returns newest-first; replay oldest-first like the TS driver does.
        StringBuilder text = new StringBuilder();
        for (int i = entries.size() - 1; i >= 0; i--) {
            String raw = entries.get(i).getFields().get(Constants.RedisFields.DATA);
            if (raw == null) {
                continue;
            }
            try {
                JsonNode payload = mapper.readTree(raw);
                JsonNode content = payload.path("data").path("choices").path(0).path("delta").path("content");
                if (content.isTextual()) {
                    text.append(content.asText());
                }
            } catch (Exception e) {
                // skip unparseable entries
            }
        }
        return text.toString();
    }

    private static boolean waitForEchoedReply(RedisClient redisClient, ObjectMapper mapper) throws InterruptedException {
        String streamName = Constants.QueueNames.sessionDataStream(SESSION_ID);
        long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (reconstructReplyText(redisClient, mapper, streamName).contains(EXPECTED_FRAGMENT)) {
                return true;
            }
            Thread.sleep(POLL_INTERVAL_MS);
        }
        return false;
    }

    public static void main(String[] args) throws InterruptedException {
        RedisClient redisClient = RedisClient.getInstance();
        ObjectMapper mapper = new ObjectMapper();

        GatewayClient<String> client = new GatewayClient<>(redisClient);

        System.out.println("Sending message to " + AGENT_TYPE + "...");
        GatewayClient.SendResponse response = client.sendMessage(AGENT_TYPE, SESSION_ID, "smoke test ping");

        if (!response.isSuccess()) {
            System.err.println("Send failed: " + response.getError());
            System.exit(1);
            return;
        }

        boolean found = waitForEchoedReply(redisClient, mapper);
        if (!found) {
            System.err.println("Timed out waiting for echoed reply containing: " + EXPECTED_FRAGMENT);
            System.exit(1);
            return;
        }

        System.out.println("Smoke test passed: worker echoed the expected reply.");
        redisClient.close();
    }
}
