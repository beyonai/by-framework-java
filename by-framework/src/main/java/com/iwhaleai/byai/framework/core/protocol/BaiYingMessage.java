package com.iwhaleai.byai.framework.core.protocol;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BaiYingMessage {
    private String role;
    private Object content;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MessageContent {
        private String text;
        private List<MessageFile> files;
        private List<Resource> resources;
        private Map<String, Object> extra;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MessageFile {
        private String fileId;
        private String fileUrl;
        private String fileType;
        private String fileName;
        private Long fileSize;
    }

    public enum Role {
        USER("user"),
        ASSISTANT("assistant"),
        SYSTEM("system"),
        TOOL_CALL("tool-call"),
        TOOL_RESPONSE("tool-response"),
        RESPONSE_TO_SUB_AGENT("response-to-sub-agent");

        private final String value;

        Role(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Resource {
        private String resourceId;
        private String resourceName;
        private String resourceType;
        private String id;
        private String path;
        private String resourceDesc;
        private Map<String, Object> resourceMetaData;
    }
}
