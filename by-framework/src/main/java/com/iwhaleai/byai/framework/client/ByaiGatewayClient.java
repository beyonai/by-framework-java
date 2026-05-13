package com.iwhaleai.byai.framework.client;

import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.client.interceptors.ByaiMessageInterceptor;
import com.iwhaleai.byai.framework.client.interceptors.GatewayInterceptor;

import java.util.ArrayList;
import java.util.List;

/**
 * A specialized GatewayClient for the Byai domain.
 * It automatically includes the ByaiMessageInterceptor to handle BaiYingMessage
 * objects.
 */
public class ByaiGatewayClient extends GatewayClient<Object> {

    public ByaiGatewayClient(RedisClient redisClient) {
        this(redisClient, null);
    }

    public ByaiGatewayClient(RedisClient redisClient, List<GatewayInterceptor> additionalInterceptors) {
        super(redisClient, createDefaultInterceptors(additionalInterceptors));
    }

    public ByaiGatewayClient(String host, int port) {
        this(new RedisClient(host, port));
    }

    private static List<GatewayInterceptor> createDefaultInterceptors(List<GatewayInterceptor> additionals) {
        List<GatewayInterceptor> interceptors = new ArrayList<>();
        interceptors.add(new ByaiMessageInterceptor());
        if (additionals != null) {
            interceptors.addAll(additionals);
        }
        return interceptors;
    }
}
