package com.iwhaleai.byai.framework.client;

import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.io.ByteArrayOutputStream;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LangfuseClientDispatchTracerTest {

    @Test
    void postsClientDispatchCreateAndUpdateToLangfuseIngestion() throws Exception {
        RecordingHttpClient httpClient = new RecordingHttpClient();
        LangfuseClientDispatchTracer tracer = new LangfuseClientDispatchTracer(
                Map.of(
                        "LANGFUSE_PUBLIC_KEY", "pk-test",
                        "LANGFUSE_SECRET_KEY", "sk-test",
                        "LANGFUSE_BASE_URL", "http://langfuse.local"
                ),
                httpClient,
                new com.fasterxml.jackson.databind.ObjectMapper());

        ClientDispatchObservation observation = tracer.start(
                new ClientDispatchTracer.ClientDispatchRequest(
                        "trace-client",
                        "msg-client",
                        "demo-agent",
                        "sess-1",
                        "user-1",
                        "User One",
                        "hello",
                        Map.of("request_id", "req-1"),
                        "060f92f2a4dc5da4"));
        assertNotNull(observation);
        assertEquals("060f92f2a4dc5da4", observation.id());

        observation.end(Map.of("success", true), "");

        assertEquals(2, httpClient.requests.size());
        assertEquals(URI.create("http://langfuse.local/api/public/ingestion"),
                httpClient.requests.get(0).uri());
        assertTrue(httpClient.requests.get(0).headers().firstValue("Authorization")
                .orElse("").startsWith("Basic "));
        assertTrue(httpClient.bodies.get(0).contains("\"type\":\"trace-create\""));
        assertTrue(httpClient.bodies.get(0).contains("\"type\":\"span-create\""));
        assertTrue(httpClient.bodies.get(0).contains("\"name\":\"client.dispatch:demo-agent\""));
        assertTrue(httpClient.bodies.get(0).contains("\"id\":\"060f92f2a4dc5da4\""));
        assertTrue(httpClient.bodies.get(1).contains("\"type\":\"span-update\""));
        assertTrue(httpClient.bodies.get(1).contains("\"success\":true"));
    }

    @Test
    void startReturnsNullWhenByaiLangfuseEnabledIsFalseLike() {
        for (String disabledValue : List.of("0", "false", "no", "off", "disabled")) {
            RecordingHttpClient httpClient = new RecordingHttpClient();
            LangfuseClientDispatchTracer tracer = new LangfuseClientDispatchTracer(
                    Map.of(
                            "LANGFUSE_PUBLIC_KEY", "pk-test",
                            "LANGFUSE_SECRET_KEY", "sk-test",
                            "LANGFUSE_BASE_URL", "http://langfuse.local",
                            "BYAI_LANGFUSE_ENABLED", disabledValue
                    ),
                    httpClient,
                    new com.fasterxml.jackson.databind.ObjectMapper());

            assertNull(tracer.start(request()));
            assertTrue(httpClient.requests.isEmpty());
        }
    }

    @Test
    void startRequiresLangfuseBaseUrlLikePythonClient() {
        RecordingHttpClient httpClient = new RecordingHttpClient();
        LangfuseClientDispatchTracer tracer = new LangfuseClientDispatchTracer(
                Map.of(
                        "LANGFUSE_PUBLIC_KEY", "pk-test",
                        "LANGFUSE_SECRET_KEY", "sk-test",
                        "LANGFUSE_HOST", "http://langfuse.local",
                        "LANGFUSE_INGESTION_URL", "http://langfuse.local/custom"
                ),
                httpClient,
                new com.fasterxml.jackson.databind.ObjectMapper());

        assertNull(tracer.start(request()));
        assertTrue(httpClient.requests.isEmpty());
    }

    @Test
    void startIgnoresLegacyJavaLangfuseEnabledFlags() {
        RecordingHttpClient httpClient = new RecordingHttpClient();
        LangfuseClientDispatchTracer tracer = new LangfuseClientDispatchTracer(
                Map.of(
                        "LANGFUSE_PUBLIC_KEY", "pk-test",
                        "LANGFUSE_SECRET_KEY", "sk-test",
                        "LANGFUSE_BASE_URL", "http://langfuse.local/",
                        "BY_FRAMEWORK_LANGFUSE_ENABLED", "false",
                        "LANGFUSE_ENABLED", "false"
                ),
                httpClient,
                new com.fasterxml.jackson.databind.ObjectMapper());

        ClientDispatchObservation observation = tracer.start(request());

        assertNotNull(observation);
        assertEquals(1, httpClient.requests.size());
        assertEquals(URI.create("http://langfuse.local/api/public/ingestion"),
                httpClient.requests.get(0).uri());
    }

    private static ClientDispatchTracer.ClientDispatchRequest request() {
        return new ClientDispatchTracer.ClientDispatchRequest(
                "trace-client",
                "msg-client",
                "demo-agent",
                "sess-1",
                "user-1",
                "User One",
                "hello",
                Map.of("request_id", "req-1"),
                "060f92f2a4dc5da4");
    }

    private static class RecordingHttpClient extends HttpClient {
        private final List<HttpRequest> requests = new ArrayList<>();
        private final List<String> bodies = new ArrayList<>();

        @Override
        public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler) {
            requests.add(request);
            bodies.add(readBody(request));
            return new FakeHttpResponse<>(request, 200, null);
        }

        @Override
        public <T> CompletableFuture<HttpResponse<T>> sendAsync(
                HttpRequest request,
                HttpResponse.BodyHandler<T> responseBodyHandler) {
            return CompletableFuture.completedFuture(send(request, responseBodyHandler));
        }

        @Override
        public <T> CompletableFuture<HttpResponse<T>> sendAsync(
                HttpRequest request,
                HttpResponse.BodyHandler<T> responseBodyHandler,
                HttpResponse.PushPromiseHandler<T> pushPromiseHandler) {
            return CompletableFuture.completedFuture(send(request, responseBodyHandler));
        }

        @Override
        public Optional<CookieHandler> cookieHandler() {
            return Optional.empty();
        }

        @Override
        public Optional<Duration> connectTimeout() {
            return Optional.empty();
        }

        @Override
        public Redirect followRedirects() {
            return Redirect.NEVER;
        }

        @Override
        public Optional<ProxySelector> proxy() {
            return Optional.empty();
        }

        @Override
        public SSLContext sslContext() {
            return null;
        }

        @Override
        public SSLParameters sslParameters() {
            return null;
        }

        @Override
        public Optional<Authenticator> authenticator() {
            return Optional.empty();
        }

        @Override
        public Version version() {
            return Version.HTTP_1_1;
        }

        @Override
        public Optional<Executor> executor() {
            return Optional.empty();
        }

        private static String readBody(HttpRequest request) {
            return request.bodyPublisher()
                    .map(RecordingHttpClient::readPublisher)
                    .orElse("");
        }

        private static String readPublisher(HttpRequest.BodyPublisher publisher) {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            CountDownLatch done = new CountDownLatch(1);
            publisher.subscribe(new Flow.Subscriber<>() {
                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    subscription.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(ByteBuffer item) {
                    byte[] bytes = new byte[item.remaining()];
                    item.get(bytes);
                    output.write(bytes, 0, bytes.length);
                }

                @Override
                public void onError(Throwable throwable) {
                    done.countDown();
                }

                @Override
                public void onComplete() {
                    done.countDown();
                }
            });
            try {
                done.await(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return output.toString(StandardCharsets.UTF_8);
        }
    }

    private record FakeHttpResponse<T>(HttpRequest request, int statusCode, T body)
            implements HttpResponse<T> {
        @Override
        public Optional<HttpResponse<T>> previousResponse() {
            return Optional.empty();
        }

        @Override
        public HttpHeaders headers() {
            return HttpHeaders.of(Map.of(), (key, value) -> true);
        }

        @Override
        public Optional<SSLSession> sslSession() {
            return Optional.empty();
        }

        @Override
        public URI uri() {
            return request.uri();
        }

        @Override
        public HttpClient.Version version() {
            return HttpClient.Version.HTTP_1_1;
        }
    }
}
