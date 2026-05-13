package com.iwhaleai.byai.framework.core.extensions;

public enum ConflictStrategy {
    ERROR("error"),
    OVERWRITE("overwrite"),
    SKIP("skip");

    private final String value;

    ConflictStrategy(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
