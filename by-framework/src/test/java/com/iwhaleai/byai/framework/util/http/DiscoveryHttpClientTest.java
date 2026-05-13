package com.iwhaleai.byai.framework.util.http;

import com.iwhaleai.byai.framework.core.discovery.DiscoveryClient;
import com.iwhaleai.byai.framework.core.discovery.ServiceInstance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DiscoveryHttpClientTest {

    @Mock
    private DiscoveryClient discoveryClient;

    @Mock
    private ByHttpClient httpClient;

    private DiscoveryHttpClient discoveryHttpClient;

    @BeforeEach
    void setUp() {
        discoveryHttpClient = new DiscoveryHttpClient(discoveryClient, httpClient, null);
    }

    @Test
    void testUrlConstructionWithProtocolAndPrefix() throws Exception {
        String serviceName = "test-service";
        ServiceInstance instance = ServiceInstance.builder()
                .id("i1")
                .protocol("https")
                .host("myservice.com")
                .port(443)
                .pathPrefix("/v1")
                .build();

        when(discoveryClient.discover(eq(serviceName), anyLong())).thenReturn(Optional.of(instance));
        
        // Mock success response
        HttpResponse mockResponse = mock(HttpResponse.class);
        when(mockResponse.isSuccess()).thenReturn(true);
        when(httpClient.request(anyString(), anyString(), any(), any(), any(), any(), anyInt()))
                .thenReturn(CompletableFuture.completedFuture(mockResponse));

        discoveryHttpClient.get(serviceName, "/users", null, null).get();

        ArgumentCaptor<String> urlCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpClient).request(eq("GET"), urlCaptor.capture(), any(), any(), any(), any(), anyInt());

        assertEquals("https://myservice.com:443/v1/users", urlCaptor.getValue());
    }

    @Test
    void testUrlConstructionWithNoPrefix() throws Exception {
        String serviceName = "test-service";
        ServiceInstance instance = ServiceInstance.builder()
                .id("i1")
                .protocol("http")
                .host("localhost")
                .port(8080)
                .pathPrefix(null)
                .build();

        when(discoveryClient.discover(eq(serviceName), anyLong())).thenReturn(Optional.of(instance));
        
        HttpResponse mockResponse = mock(HttpResponse.class);
        when(mockResponse.isSuccess()).thenReturn(true);
        when(httpClient.request(anyString(), anyString(), any(), any(), any(), any(), anyInt()))
                .thenReturn(CompletableFuture.completedFuture(mockResponse));

        discoveryHttpClient.post(serviceName, "api/save", null, null, null).get();

        ArgumentCaptor<String> urlCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpClient).request(eq("POST"), urlCaptor.capture(), any(), any(), any(), any(), anyInt());

        assertEquals("http://localhost:8080/api/save", urlCaptor.getValue());
    }

    @Test
    void testUrlConstructionWithTrailingSlashInPrefix() throws Exception {
        String serviceName = "test-service";
        ServiceInstance instance = ServiceInstance.builder()
                .id("i1")
                .protocol("http")
                .host("localhost")
                .port(8080)
                .pathPrefix("/app/")
                .build();

        when(discoveryClient.discover(eq(serviceName), anyLong())).thenReturn(Optional.of(instance));
        
        HttpResponse mockResponse = mock(HttpResponse.class);
        when(mockResponse.isSuccess()).thenReturn(true);
        when(httpClient.request(anyString(), anyString(), any(), any(), any(), any(), anyInt()))
                .thenReturn(CompletableFuture.completedFuture(mockResponse));

        discoveryHttpClient.get(serviceName, "/api", null, null).get();

        ArgumentCaptor<String> urlCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpClient).request(anyString(), urlCaptor.capture(), any(), any(), any(), any(), anyInt());

        assertEquals("http://localhost:8080/app/api", urlCaptor.getValue());
    }
}
