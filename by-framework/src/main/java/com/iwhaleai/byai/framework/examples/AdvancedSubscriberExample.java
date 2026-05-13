package com.iwhaleai.byai.framework.examples;

import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.protocol.AskAgentCommand;
import com.iwhaleai.byai.framework.core.protocol.CancelTaskCommand;
import com.iwhaleai.byai.framework.core.protocol.GatewayCommand;
import com.iwhaleai.byai.framework.worker.AgentContext;
import com.iwhaleai.byai.framework.worker.GatewayWorker;
import com.iwhaleai.byai.framework.worker.WorkerRunner;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 高级订阅器示例。
 * 演示如何处理长耗时任务、支持流式输出以及响应取消指令。
 */
public class AdvancedSubscriberExample {

    public static class MyAdvancedWorker extends GatewayWorker {
        // 用于跟踪正在运行的任务及其线程，以便取消
        private final Map<String, Thread> activeTasks = new ConcurrentHashMap<>();

        public MyAdvancedWorker(String workerId) {
            super(workerId);
        }

        @Override
        public List<String> getAgentTypes() {
            return List.of("advanced-java-agent");
        }

        @Override
        public void onCancelTask(CancelTaskCommand command) {
            String targetId = command.targetMessageId();
            Thread taskThread = activeTasks.get(targetId);
            if (taskThread != null) {
                System.out.println("[!] 收到取消指令，正在中断任务: " + targetId);
                taskThread.interrupt();
            } else {
                System.out.println("[?] 未找到活跃任务: " + targetId);
            }
        }

        @Override
        public Object processCommand(GatewayCommand command, AgentContext context) {
            if (!(command instanceof AskAgentCommand askCmd)) {
                return "Unsupported command type";
            }

            String messageId = askCmd.header().messageId();
            activeTasks.put(messageId, Thread.currentThread());

            try {
                System.out.println("[+] 开始执行长耗时任务: " + messageId);
                context.emitState("任务开始...");

                for (int i = 1; i <= 10; i++) {
                    if (Thread.currentThread().isInterrupted()) {
                        System.out.println("[!] 任务被中断: " + messageId);
                        context.emitState("任务被取消");
                        return "CANCELLED";
                    }

                    context.emitChunk("正在处理第 " + i + " 步...\n");
                    System.out.println("    进度: " + i + "/10");
                    
                    try {
                        Thread.sleep(1000); // 模拟耗时操作
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt(); // 重新标记中断
                        continue;
                    }
                }

                context.emitState("任务完成");
                return "SUCCESS";
            } finally {
                activeTasks.remove(messageId);
            }
        }
    }

    public static void main(String[] args) {
        System.out.println("=== Java SDK 高级功能演示 (Pub/Sub + 取消机制) ===");
        
        // 环境变量读取 Redis 配置 (REDIS_HOST, REDIS_PORT, etc.)
        RedisClient redisClient = RedisClient.getInstance();
        MyAdvancedWorker worker = new MyAdvancedWorker("advanced-worker-001");
        
        // WorkerRunner 内部会自动监听能力流和 Worker 专属控制流
        WorkerRunner runner = new WorkerRunner(worker, redisClient, "java-advanced-group");
        
        System.out.println("[*] 正在启动 Runner...");
        runner.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[*] 正在停止应用...");
            runner.stop();
            redisClient.close();
        }));
        
        System.out.println("[*] 运行中。按 Ctrl+C 停止。");
    }
}
