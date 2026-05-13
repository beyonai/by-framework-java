package com.iwhaleai.byai.framework.examples;

import com.iwhaleai.byai.framework.client.GatewayClient;
import com.iwhaleai.byai.framework.common.RedisClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 演示如何发送带有结构化内容 (List/Map) 的消息。
 */
public class SendStructuredMessageExample {

    public static void main(String[] args) {
        // 1. 获取 Redis 客户端 (配置会自动从 gateway-config.properties 加载)
        RedisClient redisClient = RedisClient.getInstance();
        GatewayClient client = new GatewayClient(redisClient);

        // 2. 准备结构化 Content (模拟 Python 中的 List[Dict])
        List<Map<String, Object>> messages = new ArrayList<>();

        Map<String, Object> msg1 = new HashMap<>();
        msg1.put("role", "user");
        msg1.put("content", "你好，请自我介绍。");
        messages.add(msg1);

        Map<String, Object> msg2 = new HashMap<>();
        msg2.put("role", "assistant");
        msg2.put("content", "你好！我是百应 AI 助手。");
        messages.add(msg2);

        Map<String, Object> msg3 = new HashMap<>();
        msg3.put("role", "user");
        msg3.put("content", "你能帮我写一段 Java 代码吗？");
        messages.add(msg3);

        // 4. 发送消息
        String targetAgentType = "langgraph_agent";
        String sessionId = "session-demo-a-b-a";

        System.out.println("正在向 Agent (langgraph_agent) 发送结构化消息 (List<Map>)...");

        GatewayClient.SendResponse response = client.sendMessage(
                targetAgentType,
                sessionId,
                messages // 直接传入 List
        );

        // 5. 处理结果
        if (response.isSuccess()) {
            System.out.println("✅ 消息发送成功！");
            System.out.println("   Message ID: " + response.getMessageId());
            System.out.println("   Target Worker: " + response.getTargetWorkerId());
        } else {
            System.err.println("❌ 消息发送失败！");
            System.err.println("   Error: " + response.getError());
        }

        redisClient.close();
    }
}
