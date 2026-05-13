package com.iwhaleai.byai.framework.core.extensions;

import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PluginManifest {
    private String pluginId;
    @Builder.Default
    private String version = "1.0.0";
    @Builder.Default
    private int priority = 0;
    @Builder.Default
    private boolean enabled = true;
}
