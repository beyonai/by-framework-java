package com.iwhaleai.byai.framework.core.discovery;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * 服务实例信息数据结构。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ServiceInstance {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @JsonProperty("id")
    private String id;

    @JsonProperty("protocol")
    @Builder.Default
    private String protocol = "http";
    
    @JsonProperty("host")
    private String host;
    
    @JsonProperty("port")
    private int port;

    @JsonProperty("path_prefix")
    private String pathPrefix;
    
    @JsonProperty("weight")
    @Builder.Default
    private int weight = 1;
    
    @JsonProperty("metadata")
    private Map<String, Object> metadata;

    @JsonIgnore
    private long lastHeartbeat;

    public String toJson() {
        try {
            return OBJECT_MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize ServiceInstance", e);
        }
    }

    public static ServiceInstance fromJson(String json) {
        try {
            return OBJECT_MAPPER.readValue(json, ServiceInstance.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize ServiceInstance", e);
        }
    }
}
