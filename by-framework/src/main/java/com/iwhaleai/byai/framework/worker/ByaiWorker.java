package com.iwhaleai.byai.framework.worker;

import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.WorkerRegistry;
import com.iwhaleai.byai.framework.core.protocol.AskAgentCommand;
import com.iwhaleai.byai.framework.core.protocol.ByaiContentCodec;
import com.iwhaleai.byai.framework.core.protocol.ContentCodec;
import com.iwhaleai.byai.framework.core.protocol.GatewayCommand;
import com.iwhaleai.byai.framework.core.protocol.ResumeCommand;

/** GatewayWorker variant that decodes Byai message payloads for business logic. */
public abstract class ByaiWorker extends GatewayWorker {

    public ByaiWorker(String workerId) {
        super(workerId);
    }

    public ByaiWorker(String workerId, RedisClient redisClient) {
        super(workerId, redisClient);
    }

    protected ByaiWorker(String workerId, RedisClient redisClient, WorkerRegistry registry) {
        super(workerId, redisClient, registry);
    }

    @Override
    protected ContentCodec getContentCodec() {
        return new ByaiContentCodec();
    }

    @Override
    protected AgentContext createContext(String sessionId, String traceId, RedisClient redisClient,
            String currentAgentType, String currentMessageId) {
        return new ByaiAgentContext(sessionId, traceId, redisClient, currentAgentType, currentMessageId);
    }

    @Override
    protected GatewayCommand prepareCommandForProcessing(GatewayCommand command) {
        ContentCodec codec = getContentCodec();
        if (codec == null) {
            return command;
        }

        if (command instanceof AskAgentCommand askAgentCommand) {
            Object decoded = codec.deserialize(askAgentCommand.content());
            return AskAgentCommand.of(
                    askAgentCommand.header(),
                    decoded,
                    askAgentCommand.waitForReply(),
                    askAgentCommand.extraPayload()
            );
        }
        if (command instanceof ResumeCommand resumeCommand) {
            Object decoded = codec.deserialize(resumeCommand.content());
            return ResumeCommand.of(
                    resumeCommand.header(),
                    decoded,
                    resumeCommand.status(),
                    resumeCommand.replyData(),
                    resumeCommand.extraPayload()
            );
        }
        return command;
    }
}
