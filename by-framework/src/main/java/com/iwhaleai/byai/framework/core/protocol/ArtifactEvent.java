package com.iwhaleai.byai.framework.core.protocol;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Event for artifact/file creation.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ArtifactEvent {
    private String url;
    @Builder.Default
    private Map<String, Object> metadata = Map.of();
}
