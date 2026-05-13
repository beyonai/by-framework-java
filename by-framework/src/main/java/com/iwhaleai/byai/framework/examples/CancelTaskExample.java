package com.iwhaleai.byai.framework.examples;

import com.iwhaleai.byai.framework.client.GatewayClient;
import com.iwhaleai.byai.framework.common.RedisClient;

/**
 * 演示如何通过 Java SDK 发起任务中断。
 *
 * 运行方式：
 * `java ... CancelTaskExample <messageId> <sessionId>`
 */
public class CancelTaskExample {

    public static void main(String[] args) {
        RedisClient redisClient = RedisClient.getInstance();
        GatewayClient<Object> client = new GatewayClient<>(redisClient);

        String messageId = args.length > 0 ? args[0] : "msg-to-cancel";
        String sessionId = args.length > 1 ? args[1] : "session-to-cancel";

        GatewayClient.CancelTaskResponse result = client.cancelTask(
                messageId,
                sessionId,
                "manual demo interrupt",
                "",
                "java-example",
                "graceful"
        );

        System.out.println("Cancel result: " + result);
        redisClient.close();
    }
}
