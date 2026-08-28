package com.iwhaleai.byai.framework.examples;

import com.iwhaleai.byai.framework.core.protocol.GatewayCommand;
import com.iwhaleai.byai.framework.worker.AgentContext;
import com.iwhaleai.byai.framework.worker.GatewayWorker;
import com.iwhaleai.byai.framework.worker.WorkerRunner;

import java.util.List;

/**
 * Picks a task up and never finishes it - a worker meant to be killed
 * (kill -9, not a graceful shutdown) mid-task. Used by
 * scripts/cross_language_liveness_check.py at the workspace root to prove a
 * Python caller's WaitIndexSweeper resolves a suspended caller whose Java
 * callee's process died, using nothing but the Redis worker registry both
 * SDKs share.
 */
public class HangingWorker extends GatewayWorker {

    public HangingWorker(String workerId) {
        super(workerId);
    }

    @Override
    public List<String> getAgentTypes() {
        return List.of("java-hang-callee");
    }

    @Override
    public Object processCommand(GatewayCommand command, AgentContext context) {
        System.out.println("HangingWorker started: " + command.header().messageId());
        System.out.flush();
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return "unreachable";
    }

    public static void main(String[] args) {
        HangingWorker worker = new HangingWorker("java-hang-worker-01");
        WorkerRunner runner = new WorkerRunner(worker);
        runner.start();

        Runtime.getRuntime().addShutdownHook(new Thread(runner::stop));
    }
}
