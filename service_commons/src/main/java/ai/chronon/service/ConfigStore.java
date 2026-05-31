package ai.chronon.service;

import io.vertx.config.ConfigRetriever;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import ai.chronon.online.metrics.TTLCache;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Helps keep track of the various Chronon service configs.
 * We currently read configs once at startup - this makes sense for configs
 * such as the server port and we can revisit / extend things in the future to
 * be able to hot-refresh configs like Vertx supports under the hood.
 */
public class ConfigStore {

    private static final int DEFAULT_PORT = 8080;

    private static final String SERVER_PORT = "server.port";
    private static final String ONLINE_JAR = "online.jar";
    private static final String ONLINE_CLASS = "online.class";
    private static final String ONLINE_API_PROPS = "online.api.props";
    
    // Database configuration
    private static final String JDBC_URL = "db.url";
    private static final String JDBC_USERNAME = "db.username";
    private static final String JDBC_PASSWORD = "db.password";
    
    // GCP configuration
    private static final String GCP_PROJECT_ID = "gcp.projectId";

    // TTL cache configuration
    private static final String JOIN_CONF_TTL_MILLIS = "ai.chronon.join.conf.ttl.millis";
    private static final String JOIN_CODEC_TTL_MILLIS = "ai.chronon.join.codec.ttl.millis";

    private volatile JsonObject jsonConfig;
    private final Object lock = new Object();

    public ConfigStore(Vertx vertx) {
        // Use CountDownLatch to wait for config loading
        CountDownLatch latch = new CountDownLatch(1);
        ConfigRetriever configRetriever = ConfigRetriever.create(vertx);
        configRetriever.getConfig().onComplete(ar -> {
            if (ar.failed()) {
                throw new IllegalStateException("Unable to load service config", ar.cause());
            }
            synchronized (lock) {
                jsonConfig = ar.result();
            }
            latch.countDown();
        });
        try {
            if (!latch.await(1, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for Vertx config read");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while loading config", e);
        }
    }

    public int getServerPort() {
        return jsonConfig.getInteger(SERVER_PORT, DEFAULT_PORT);
    }

    public Optional<String> getOnlineJar() {
        return Optional.ofNullable(jsonConfig.getString(ONLINE_JAR));
    }

    public Optional<String> getOnlineClass() {
        return Optional.ofNullable(jsonConfig.getString(ONLINE_CLASS));
    }

    public void validateOnlineApiConfig() {
        if (!(getOnlineJar().isPresent() && getOnlineClass().isPresent())) {
            throw new IllegalArgumentException("Both 'online.jar' and 'online.class' configs must be set.");
        }
    }

    public Map<String, String> getOnlineApiProps() {
        JsonObject apiProps = jsonConfig.getJsonObject(ONLINE_API_PROPS);
        if (apiProps == null) {
            return new HashMap<String, String>();
        }

        return apiProps.stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> String.valueOf(e.getValue())
        ));
    }

    /**
     * Gets the JDBC URL for database connection.
     * 
     * @return the JDBC URL
     */
    public String getJdbcUrl() {
        return jsonConfig.getString(JDBC_URL);
    }
    
    /**
     * Gets the JDBC username for database connection.
     * 
     * @return the JDBC username
     */
    public String getJdbcUsername() {
        return jsonConfig.getString(JDBC_USERNAME);
    }
    
    /**
     * Gets the JDBC password for database connection.
     * 
     * @return the JDBC password
     */
    public String getJdbcPassword() {
        return jsonConfig.getString(JDBC_PASSWORD);
    }
    
    /**
     * Gets the GCP project ID.
     * 
     * @return the GCP project ID
     */
    public String getGcpProjectId() {
        return jsonConfig.getString(GCP_PROJECT_ID);
    }
    
    /**
     * Validates database configuration.
     * Ensures all required database properties are set.
     * 
     * @throws IllegalArgumentException if any required property is missing
     */
    public void validateDatabaseConfig() {
        if (getJdbcUrl() == null || getJdbcUrl().trim().isEmpty()) {
            throw new IllegalArgumentException("Database URL is required. Please set 'db.url'.");
        }
        if (getJdbcUsername() == null || getJdbcUsername().trim().isEmpty()) {
            throw new IllegalArgumentException("Database username is required. Please set 'db.username'.");
        }
        if (getJdbcPassword() == null || getJdbcPassword().trim().isEmpty()) {
            throw new IllegalArgumentException("Database password is required. Please set 'db.password'.");
        }
    }
    
    /**
     * Validates GCP configuration.
     * Ensures all required GCP properties are set.
     * 
     * @throws IllegalArgumentException if any required property is missing
     */
    public void validateGcpConfig() {
        if (getGcpProjectId() == null || getGcpProjectId().trim().isEmpty()) {
            throw new IllegalArgumentException("GCP project ID is required. Please set 'gcp.projectId'.");
        }
    }
    
    /**
     * Validates all required configuration.
     * This includes database and GCP configurations.
     * 
     * @throws IllegalArgumentException if any required configuration is invalid
     */
    public void validateAllConfig() {
        validateDatabaseConfig();
        validateGcpConfig();
    }

    public long getJoinConfTtlMillis() {
        return jsonConfig.getLong(JOIN_CONF_TTL_MILLIS, TTLCache.DefaultTtlMillis());
    }

    public long getJoinCodecTtlMillis() {
        return jsonConfig.getLong(JOIN_CODEC_TTL_MILLIS, TTLCache.DefaultTtlMillis());
    }

    public String encodeConfig() {
        return jsonConfig.encodePrettily();
    }
}
