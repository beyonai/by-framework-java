package com.iwhaleai.byai.framework.util.http;

import org.apache.http.HttpRequest;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Basic authentication (username/password).
 */
public class BasicAuth implements Auth {

    private final String username;
    private final String password;

    public BasicAuth(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public void apply(HttpRequest request) {
        String credentials = username + ":" + password;
        String encoded = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        request.setHeader("Authorization", "Basic " + encoded);
    }
}
