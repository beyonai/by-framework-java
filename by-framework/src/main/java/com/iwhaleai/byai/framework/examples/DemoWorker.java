package com.iwhaleai.byai.framework.examples;

import com.iwhaleai.byai.framework.core.protocol.AskAgentCommand;
import com.iwhaleai.byai.framework.core.protocol.GatewayCommand;
import com.iwhaleai.byai.framework.worker.AgentContext;
import com.iwhaleai.byai.framework.worker.GatewayWorker;
import com.iwhaleai.byai.framework.worker.WorkerRunner;

import java.util.List;

/**
 * Java SDK 示例 Worker 实现。
 */
public class DemoWorker extends GatewayWorker {

    public DemoWorker(String workerId) {
        super(workerId);
    }

    @Override
    public List<String> getAgentTypes() {
        return List.of("java-agent-demo");
    }

    @Override
    public Object processCommand(GatewayCommand command, AgentContext context) {
        AskAgentCommand askAgentCommand = (AskAgentCommand) command;
        System.out.println("Java Worker received task: " + askAgentCommand.content());

        context.emitState("Thinking...");
        context.emitChunk("Hello from Java SDK! I received your message: " + askAgentCommand.content());
        
        return "Task processed by Java";
    }

    public static void main(String[] args) {
        DemoWorker worker = new DemoWorker("java-worker-001");
        WorkerRunner runner = new WorkerRunner(worker);
        runner.start();
        
        Runtime.getRuntime().addShutdownHook(new Thread(runner::stop));
    }
}
