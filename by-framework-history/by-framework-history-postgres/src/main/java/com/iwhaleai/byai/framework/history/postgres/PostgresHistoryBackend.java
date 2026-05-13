package com.iwhaleai.byai.framework.history.postgres;

import com.iwhaleai.byai.framework.core.runtime.history.BaseHistoryBackend;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * PostgreSQL history backend with connection pooling.
 * Uses JDBC for database operations.
 */
@Slf4j
public class PostgresHistoryBackend extends BaseHistoryBackend {

    private static final String CREATE_TABLE_SQL =
            """
            CREATE TABLE IF NOT EXISTS gateway_session_messages (
                id BIGSERIAL PRIMARY KEY,
                session_id VARCHAR(128) NOT NULL,
                role VARCHAR(32) NOT NULL CHECK (role IN ('user', 'assistant', 'system', 'tool')),
                content TEXT NOT NULL,
                metadata JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """;

    private static final String CREATE_INDEX_SQL =
            """
            CREATE INDEX IF NOT EXISTS idx_gateway_session_messages_session_created
            ON gateway_session_messages (session_id, created_at DESC, id DESC)
            """;

    private static final String INSERT_SQL =
            "INSERT INTO gateway_session_messages (session_id, role, content, metadata) VALUES (?, ?, ?, ?::jsonb)";

    private static final String SELECT_SQL =
            """
            SELECT role, content, metadata
            FROM gateway_session_messages
            WHERE session_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """;

    private final String dsn;
    private final int minSize;
    private final int maxSize;
    private final String username;
    private final String password;
    private final List<Connection> pool = Collections.synchronizedList(new ArrayList<>());
    private final Set<Connection> usedConnections = Collections.synchronizedSet(new HashSet<>());
    private boolean initialized = false;

    public PostgresHistoryBackend(String dsn, int minSize, int maxSize, String username, String password) {
        this.dsn = dsn != null ? dsn : System.getenv("BYAI_HISTORY_PG_DSN");
        this.minSize = minSize > 0 ? minSize : 1;
        this.maxSize = maxSize > 0 ? maxSize : 10;
        this.username = username;
        this.password = password;
    }

    public PostgresHistoryBackend(String dsn) {
        this(dsn, 1, 10, null, null);
    }

    @Override
    public List<Map<String, Object>> getHistory(String sessionId, int limit) {
        ensureInitialized();
        List<Map<String, Object>> result = new ArrayList<>();

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(SELECT_SQL)) {

            stmt.setString(1, sessionId);
            stmt.setInt(2, limit);

            try (ResultSet rs = stmt.executeQuery()) {
                // Results come in reverse chronological order, so we reverse to get oldest first
                LinkedList<Map<String, Object>> temp = new LinkedList<>();
                while (rs.next()) {
                    Map<String, Object> message = new HashMap<>();
                    message.put("role", rs.getString("role"));
                    message.put("content", rs.getString("content"));
                    message.put("metadata", parseMetadata(rs.getString("metadata")));
                    temp.addFirst(message);
                }
                result.addAll(temp);
            }
        } catch (SQLException e) {
            log.error("Failed to get history for session: {}", sessionId, e);
            return Collections.emptyList();
        }

        return result;
    }

    @Override
    public void saveMessage(String sessionId, String role, String content, Map<String, Object> metadata) {
        ensureInitialized();

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(INSERT_SQL)) {

            stmt.setString(1, sessionId);
            stmt.setString(2, role);
            stmt.setString(3, content);
            stmt.setString(4, metadataToJson(metadata));

            stmt.executeUpdate();
            log.debug("Saved message to PostgreSQL: session={}, role={}", sessionId, role);
        } catch (SQLException e) {
            log.error("Failed to save message for session: {}", sessionId, e);
        }
    }

    /**
     * Close all connections in the pool.
     */
    public void close() {
        synchronized (pool) {
            for (Connection conn : pool) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.warn("Error closing connection", e);
                }
            }
            pool.clear();
        }
    }

    private void ensureInitialized() {
        if (!initialized) {
            synchronized (this) {
                if (!initialized) {
                    initializePool();
                    initialized = true;
                }
            }
        }
    }

    private void initializePool() {
        try {
            // Create initial connections
            for (int i = 0; i < minSize; i++) {
                pool.add(createConnection());
            }

            // Create schema
            try (Connection conn = getConnection();
                 Statement stmt = conn.createStatement()) {
                stmt.execute(CREATE_TABLE_SQL);
                stmt.execute(CREATE_INDEX_SQL);
                log.info("PostgreSQL history schema initialized");
            }
        } catch (SQLException e) {
            log.error("Failed to initialize connection pool", e);
            throw new RuntimeException("Failed to initialize PostgreSQL history backend", e);
        }
    }

    private Connection getConnection() throws SQLException {
        // Try to get an available connection from pool
        while (!pool.isEmpty()) {
            Connection conn = pool.remove(pool.size() - 1);
            if (isConnectionValid(conn)) {
                usedConnections.add(conn);
                return conn;
            }
        }

        // Create new connection if under max
        if (usedConnections.size() < maxSize) {
            Connection conn = createConnection();
            usedConnections.add(conn);
            return conn;
        }

        // Wait for a connection to become available
        throw new SQLException("Connection pool exhausted");
    }

    private Connection createConnection() throws SQLException {
        Properties props = new Properties();
        if (username != null) {
            props.setProperty("user", username);
        }
        if (password != null) {
            props.setProperty("password", password);
        }
        return DriverManager.getConnection(dsn, props);
    }

    private boolean isConnectionValid(Connection conn) {
        try {
            return conn.isValid(5);
        } catch (SQLException e) {
            return false;
        }
    }

    private Map<String, Object> parseMetadata(String json) {
        if (json == null || json.isEmpty() || json.equals("{}")) {
            return Collections.emptyMap();
        }
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> result = new com.fasterxml.jackson.databind.ObjectMapper().readValue(json, Map.class);
            return result;
        } catch (Exception e) {
            log.warn("Failed to parse metadata JSON: {}", json);
            return Collections.emptyMap();
        }
    }

    private String metadataToJson(Map<String, Object> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return "{}";
        }
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(metadata);
        } catch (Exception e) {
            log.warn("Failed to serialize metadata");
            return "{}";
        }
    }
}
