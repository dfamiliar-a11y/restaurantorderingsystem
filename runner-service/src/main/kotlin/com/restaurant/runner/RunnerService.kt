package com.restaurant.runner

import com.rabbitmq.client.ConnectionFactory
import com.restaurant.common.RabbitConnectionSupport
import com.restaurant.common.RestaurantRabbitTopology
import com.restaurant.common.config.ConfigLoader
import com.restaurant.common.consumeManualAck
import com.restaurant.common.events.DeliveryConfirmedEvent
import com.restaurant.common.events.DishPreparedEvent
import com.restaurant.common.jsonPersistentMessageProperties
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory

/**
 * Runner **subscriber** process: consumes [DishPreparedEvent] from [RestaurantRabbitTopology.QUEUE_RUNNER],
 * simulates walking the order to the table, then publishes [DeliveryConfirmedEvent] so Ordering
 * can mark the aggregate [OrderStatus.SERVED].
 *
 * Delivery delay uses [Thread.sleep] on the consumer thread (with `basicQos(1)`) instead of
 * `runBlocking`, which would block a dispatcher while still tying up this thread.
 */
fun main() {
    val log = LoggerFactory.getLogger("RunnerService")
    val json = Json {
        ignoreUnknownKeys = true
        encodeDefaults = true
    }

    val mq = ConfigLoader.load().rabbitMq
    val ordersExchange = mq.exchangeName
    val factory = ConnectionFactory().apply {
        host = mq.host
        port = mq.port
        username = mq.username
        password = mq.password
        virtualHost = mq.virtualHost
    }

    val connection = RabbitConnectionSupport.connectWithRetry(factory) { log.info(it) }
    val channel = connection.createChannel().also {
        RestaurantRabbitTopology.declareRunnerTopology(it, ordersExchange)
        it.basicQos(1)
    }

    channel.consumeManualAck(
        RestaurantRabbitTopology.QUEUE_RUNNER,
        log = log,
        errorMessage = "Runner failed to process message",
    ) { _, body ->
        val dish = json.decodeFromString<DishPreparedEvent>(String(body, Charsets.UTF_8))
        log.info(
            "Runner delivering [{}] to Table [{}]",
            dish.dishSummary,
            dish.tableNumber,
        )

        // TODO: Implement actual delivery logic here (another http request to the ordering-api)
        // Simulate delivery time without blocking a coroutine dispatcher on this single-thread consumer.
        Thread.sleep(200)

        val confirmed = DeliveryConfirmedEvent(dish.orderId)
        basicPublish(
            ordersExchange,
            RestaurantRabbitTopology.ROUTING_DELIVERY_CONFIRMED,
            jsonPersistentMessageProperties(),
            json.encodeToString(confirmed).toByteArray(Charsets.UTF_8),
        )
    }

    log.info("Runner service listening on queue {}", RestaurantRabbitTopology.QUEUE_RUNNER)
    Runtime.getRuntime().addShutdownHook(
        Thread {
            runCatching { channel.close() }
            runCatching { connection.close(5_000) }
        },
    )
    Thread.currentThread().join()
}
