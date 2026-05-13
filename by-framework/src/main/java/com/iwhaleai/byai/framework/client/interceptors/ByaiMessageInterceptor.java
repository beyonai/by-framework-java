package com.iwhaleai.byai.framework.client.interceptors;

import com.iwhaleai.byai.framework.core.protocol.BaiYingMessage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Interceptor that handles conversion of domain-specific BaiYingMessage objects
 * into protocol-compatible formats (List<Map>).
 */
public class ByaiMessageInterceptor implements GatewayInterceptor {

    @Override
    public SendMessageParams beforeSend(SendMessageParams params) {
        Object content = params.getContent();
        if (content == null) {
            return params;
        }

        params.setContent(formatContent(content));
        return params;
    }

    private Object formatContent(Object content) {
        if (content instanceof String) {
            return content;
        }

        List<Object> inputList;
        if (content instanceof List) {
            inputList = (List<Object>) content;
        } else {
            inputList = new ArrayList<>();
            inputList.add(content);
        }

        List<Map<String, Object>> formattedContent = new ArrayList<>();
        for (Object m : inputList) {
            if (m instanceof Map) {
                formattedContent.add((Map<String, Object>) m);
            } else if (m instanceof BaiYingMessage) {
                BaiYingMessage message = (BaiYingMessage) m;
                Map<String, Object> map = new HashMap<>();
                map.put("role", message.getRole());
                
                Object msgContent = message.getContent();
                if (msgContent instanceof BaiYingMessage.MessageContent) {
                    BaiYingMessage.MessageContent c = (BaiYingMessage.MessageContent) msgContent;
                    Map<String, Object> contentMap = new HashMap<>();
                    contentMap.put("text", c.getText());
                    
                    List<Map<String, Object>> files = new ArrayList<>();
                    if (c.getFiles() != null) {
                        for (BaiYingMessage.MessageFile f : c.getFiles()) {
                            Map<String, Object> fileMap = new HashMap<>();
                            fileMap.put("fileId", f.getFileId());
                            fileMap.put("fileUrl", f.getFileUrl());
                            fileMap.put("fileType", f.getFileType());
                            fileMap.put("fileName", f.getFileName());
                            fileMap.put("fileSize", f.getFileSize());
                            files.add(fileMap);
                        }
                    }
                    contentMap.put("files", files);
                    contentMap.put("resources", new ArrayList<>()); // Placeholder for resources
                    
                    map.put("content", contentMap);
                } else {
                    map.put("content", msgContent);
                }
                formattedContent.add(map);
            } else {
                // Fallback for unknown types if needed, or just add as-is
                Map<String, Object> fallback = new HashMap<>();
                fallback.put("content", m.toString());
                formattedContent.add(fallback);
            }
        }
        return formattedContent;
    }
}
