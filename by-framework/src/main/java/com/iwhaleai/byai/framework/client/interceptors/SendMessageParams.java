package com.iwhaleai.byai.framework.client.interceptors;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SendMessageParams {
    private String targetAgentType;
    private String sessionId;
    private Object content;
    private String userCode;
    private String userName;
    private String actionType;
    private String parentMessageId;
    
    @Builder.Default
    private Map<String, Object> payload = new HashMap<>();
    
    @Builder.Default
    private Map<String, Object> metadata = new HashMap<>();
}
