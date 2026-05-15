package com.restaurant.kitchen

import com.rabbitmq.client.ConnectionFactory
import com.restaurant.common.RabbitConnectionSupport
import com.restaurant.common.RestaurantRabbitTopology
import com.restaurant.common.config.ConfigLoader
import com.restaurant.common.consumeManualAck
import com.restaurant.common.events.OrderPlacedEvent
import com.restaurant.common.events.OrderPreparingEvent
import com.restaurant.common.jsonPersistentMessageProperties
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory

/**
 * Kitchen **subscriber** process: consumes [OrderPlacedEvent] from [RestaurantRabbitTopology.QUEUE_KITCHEN],
 * publishes [OrderPreparingEvent] (`order.preparing`) so Ordering moves to `PREPARING`.
 *
 * Staff mark food ready for handoff via **ordering-api** `POST /order/{id}/ready-to-serve`, which
 * publishes [com.restaurant.common.events.DishPreparedEvent] (`dish.ready`) — not this service.
 *
 * `basicQos(1)` limits in-flight work per channel—appropriate when each message represents a whole order.
 */
fun main() {
    val log = LoggerFactory.getLogger("KitchenService")
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
        RestaurantRabbitTopology.declareKitchenTopology(it, ordersExchange)
        it.basicQos(1)
    }

    channel.consumeManualAck(
        RestaurantRabbitTopology.QUEUE_KITCHEN,
        log = log,
        errorMessage = "Kitchen failed to process message",
    ) { _, body ->
        val placed = json.decodeFromString<OrderPlacedEvent>(String(body, Charsets.UTF_8))
        log.info("Kitchen received order {}", placed.orderId)

        val preparing = OrderPreparingEvent(placed.orderId)
        basicPublish(
            ordersExchange,
            RestaurantRabbitTopology.ROUTING_ORDER_PREPARING,
            jsonPersistentMessageProperties(),
            json.encodeToString(preparing).toByteArray(Charsets.UTF_8),
        )
    }

    log.info("Kitchen service listening on queue {}", RestaurantRabbitTopology.QUEUE_KITCHEN)
    Runtime.getRuntime().addShutdownHook(
        Thread {
            runCatching { channel.close() }
            runCatching { connection.close(5_000) }
        },
    )
    Thread.currentThread().join()
}
