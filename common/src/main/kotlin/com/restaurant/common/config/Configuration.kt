package com.restaurant.common.config

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

/** JDBC URL, driver class, and optional credentials for Exposed. */
data class DatabaseConfig(
    val url: String,
    val driver: String,
    val user: String?,
    val password: String?,
)

/** Broker endpoint and credentials (exchange name is validated/logged for parity with [com.restaurant.common.RestaurantRabbitTopology]). */
data class RabbitMqConfig(
    val host: String,
    val port: Int,
    val username: String,
    val password: String,
    val exchangeName: String,
    val virtualHost: String,
)

/** Embedded Netty listen port for Ktor. */
data class ServerConfig(
    val port: Int,
)

/** Aggregated runtime settings loaded once at process startup. */
data class AppConfig(
    val database: DatabaseConfig,
    val rabbitMq: RabbitMqConfig,
    val server: ServerConfig,
)

/**
 * Loads configuration from classpath `application.conf` (HOCON) under `restaurant.*`, merged with
 * process environment variables (Compose, Kubernetes, CI, shell).
 *
 * Precedence per key: `System.getenv` (when set and non-blank) → HOCON default.
 */
object ConfigLoader {
    private val log = LoggerFactory.getLogger(ConfigLoader::class.java)

    /**
     * Fully merged Typesafe [Config] (classpath + known env overlays). Use for Ktor
     * [io.ktor.server.config.HoconApplicationConfig] so HTTP settings stay in sync with [AppConfig].
     */
    fun resolvedRootConfig(): Config = buildResolvedConfig()

    /**
     * Same as [load] but reuses an already-resolved root (avoids parsing classpath config twice).
     *
     * @throws IllegalStateException when required strings are blank or numeric env vars are invalid
     */
    fun load(root: Config): AppConfig {
        val appConfig = appConfigFrom(root)
        validate(appConfig)
        logLoaded(appConfig)
        return appConfig
    }

    /**
     * @throws IllegalStateException when required strings are blank or numeric env vars are invalid
     */
    fun load(): AppConfig = load(buildResolvedConfig())

    private fun buildResolvedConfig(): Config {
        val base = ConfigFactory.load()
        val overrides = envOverridesAsFlatConfigMap()
        if (overrides.isEmpty()) {
            return base.resolve()
        }
        return ConfigFactory.parseMap(overrides).withFallback(base).resolve()
    }

    private fun String?.nullIfBlank(): String? = this?.takeIf { it.isNotBlank() }

    /** Non-blank process environment value, if present. */
    private fun envString(key: String): String? = System.getenv(key).nullIfBlank()

    /**
     * Dotted paths under [restaurant] for [ConfigFactory.parseMap] (Typesafe treats `.` as nesting).
     */
    private fun envOverridesAsFlatConfigMap(): Map<String, Any> {
        val m = mutableMapOf<String, Any>()
        envString("DATABASE_URL")?.let { m["restaurant.database.url"] = it }
        envString("DATABASE_DRIVER")?.let { m["restaurant.database.driver"] = it }
        envString("DATABASE_USER")?.let { m["restaurant.database.user"] = it }
        envString("DATABASE_PASSWORD")?.let { m["restaurant.database.password"] = it }
        envString("RABBITMQ_HOST")?.let { m["restaurant.rabbitMq.host"] = it }
        envString("RABBITMQ_PORT")?.toIntOrNull()?.let { m["restaurant.rabbitMq.port"] = it }
        envString("RABBITMQ_USER")?.let { m["restaurant.rabbitMq.username"] = it }
        envString("RABBITMQ_PASSWORD")?.let { m["restaurant.rabbitMq.password"] = it }
        envString("RABBITMQ_EXCHANGE")?.let { m["restaurant.rabbitMq.exchangeName"] = it }
        envString("RABBITMQ_VHOST")?.let { m["restaurant.rabbitMq.virtualHost"] = it }
        envString("PORT")?.toIntOrNull()?.let { m["restaurant.server.port"] = it }
        return m
    }

    private fun appConfigFrom(root: Config): AppConfig {
        val r = root.getConfig("restaurant")
        val databaseUrl = r.getString("database.url")
        val databaseDriver = r.getString("database.driver")
        val databaseUser = r.optionalString("database.user")
        val databasePassword = r.optionalString("database.password")

        val rabbitHost = r.getString("rabbitMq.host")
        val rabbitPort = r.getInt("rabbitMq.port")
        val rabbitUser = r.getString("rabbitMq.username")
        val rabbitPassword = r.getString("rabbitMq.password")
        val rabbitExchange = r.getString("rabbitMq.exchangeName")
        val rabbitVHost = r.getString("rabbitMq.virtualHost")

        val serverPort = r.getInt("server.port")

        if (rabbitPort !in 1..65535) {
            throw IllegalStateException("RABBITMQ_PORT out of range: $rabbitPort")
        }
        if (serverPort !in 1..65535) {
            throw IllegalStateException("PORT out of range: $serverPort")
        }

        return AppConfig(
            database = DatabaseConfig(
                url = databaseUrl,
                driver = databaseDriver,
                user = databaseUser,
                password = databasePassword,
            ),
            rabbitMq = RabbitMqConfig(
                host = rabbitHost,
                port = rabbitPort,
                username = rabbitUser,
                password = rabbitPassword,
                exchangeName = rabbitExchange,
                virtualHost = rabbitVHost,
            ),
            server = ServerConfig(port = serverPort),
        )
    }

    private fun Config.optionalString(path: String): String? =
        when {
            !hasPath(path) -> null
            getIsNull(path) -> null
            else -> getString(path).nullIfBlank()
        }

    private fun validate(config: AppConfig) {
        if (config.database.url.isBlank()) {
            throw IllegalStateException("DATABASE_URL must not be blank")
        }
        if (config.database.driver.isBlank()) {
            throw IllegalStateException("DATABASE_DRIVER must not be blank")
        }
        if (config.rabbitMq.host.isBlank()) {
            throw IllegalStateException("RABBITMQ_HOST must not be blank")
        }
        if (config.rabbitMq.exchangeName.isBlank()) {
            throw IllegalStateException("RABBITMQ_EXCHANGE / restaurant.rabbitMq.exchangeName must not be blank")
        }
    }

    private fun logLoaded(config: AppConfig) {
        val dbKind = when {
            config.database.url.contains(":mem:", ignoreCase = true) -> "h2_mem"
            config.database.url.startsWith("jdbc:h2:file:", ignoreCase = true) -> "h2_file"
            else -> "other"
        }
        log.info(
            "Config loaded successfully: server.port={}, rabbit.host={}, rabbit.port={}, rabbit.exchangeName={}, database.driver={}, database.storageKind={}",
            config.server.port,
            config.rabbitMq.host,
            config.rabbitMq.port,
            config.rabbitMq.exchangeName,
            config.database.driver,
            dbKind,
        )
    }
}
