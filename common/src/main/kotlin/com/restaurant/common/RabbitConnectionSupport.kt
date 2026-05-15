package com.restaurant.common

import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import kotlin.math.min

/**
 * Shared connection bootstrap with exponential backoff for containerized deployments where
 * RabbitMQ may briefly lag the application on startup.
 */
object RabbitConnectionSupport {
    private const val DEFAULT_MAX_ATTEMPTS = 12
    private const val INITIAL_BACKOFF_MS = 500L
    private const val MAX_BACKOFF_MS = 10_000L

    /**
     * @param factory broker connection settings from the calling service
     * @param maxAttempts upper bound on connection attempts before failing fast
     * @param log hook for structured or plain logging at each attempt
     * @return an open [Connection] ready for channel creation
     * @throws Exception the last connection failure if all attempts are exhausted
     */
    fun connectWithRetry(
        factory: ConnectionFactory,
        maxAttempts: Int = DEFAULT_MAX_ATTEMPTS,
        log: (String) -> Unit = { println(it) },
    ): Connection {
        var attempt = 0
        var backoff = INITIAL_BACKOFF_MS
        var last: Exception? = null
        while (attempt < maxAttempts) {
            try {
                log("RabbitMQ: connecting to ${factory.host}:${factory.port} (attempt ${attempt + 1}/$maxAttempts)")
                return factory.newConnection()
            } catch (e: Exception) {
                last = e
                attempt++
                log("RabbitMQ: connection failed (${e.message}), retrying in ${backoff}ms")
                Thread.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_MS)
            }
        }
        throw last ?: IllegalStateException("RabbitMQ: exhausted retries")
    }
}
