package com.restaurant.ordering.infrastructure.messaging

import com.rabbitmq.client.Channel
import com.restaurant.common.OrderStatus
import com.restaurant.common.RestaurantRabbitTopology
import com.restaurant.common.consumeManualAck
import com.restaurant.common.events.DeliveryConfirmedEvent
import com.restaurant.common.events.DishPreparedEvent
import com.restaurant.common.events.OrderPreparingEvent
import com.restaurant.ordering.domain.ApplyLifecycleStatusResult
import com.restaurant.ordering.domain.OrderRepository
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory

/**
 * **Inbound messaging adapter** for the Ordering bounded context: translates asynchronous
 * lifecycle signals from Kitchen and Runner into [OrderRepository] writes.
 *
 * Flow (event-driven):
 * - Kitchen publishes [OrderPreparingEvent] after handling [com.restaurant.common.events.OrderPlacedEvent].
 * - Ordering API publishes [DishPreparedEvent] (`dish.ready`) when staff POSTs ready-to-serve; Runner
 *   consumes the same routing key for physical delivery.
 * - Runner publishes [DeliveryConfirmedEvent] after handling [DishPreparedEvent].
 * - This consumer keeps the authoritative order status in the Ordering database aligned with those facts,
 *   so `GET /order/{id}` reflects the distributed pipeline without synchronous coupling between services.
 *
 * **Ack policy:** successful handling issues `basicAck`. Failures use `basicNack` with requeue only for
 * likely-transient errors; **kotlinx.serialization** failures skip requeue so poison payloads route to
 * the queue's **dead-letter** arguments declared in [RestaurantRabbitTopology].
 *
 * **Idempotency:** [OrderRepository.applyLifecycleStatus] only moves forward (or stays) in the lifecycle,
 * so Rabbit's at-least-once redeliveries do not regress status (for example SERVED → READY_TO_SERVE).
 */
class OrderingUpdatesConsumer(
    private val channel: Channel,
    private val orderRepository: OrderRepository,
    private val json: Json = Json { ignoreUnknownKeys = true },
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * Subscribes to [RestaurantRabbitTopology.QUEUE_ORDERING_UPDATES]; returns the broker **consumer tag**
     * so the composition root can cancel the consumer during graceful shutdown.
     */
    fun start(): String {
        val consumerTag = channel.consumeManualAck(
            RestaurantRabbitTopology.QUEUE_ORDERING_UPDATES,
            onFailure = { env, e ->
                log.error("Failed to process message rk={}", env.routingKey ?: "", e)
            },
        ) { envelope, body ->
            val routingKey = envelope.routingKey ?: ""
            val text = String(body, Charsets.UTF_8)
            when (routingKey) {
                RestaurantRabbitTopology.ROUTING_ORDER_PREPARING -> {
                    val event = json.decodeFromString<OrderPreparingEvent>(text)
                    applyAndLog(event.orderId, OrderStatus.PREPARING)
                }
                RestaurantRabbitTopology.ROUTING_DISH_READY -> {
                    val event = json.decodeFromString<DishPreparedEvent>(text)
                    applyAndLog(event.orderId, OrderStatus.READY_TO_SERVE)
                }
                RestaurantRabbitTopology.ROUTING_DELIVERY_CONFIRMED -> {
                    val event = json.decodeFromString<DeliveryConfirmedEvent>(text)
                    applyAndLog(event.orderId, OrderStatus.SERVED)
                }
                else -> log.warn("Unknown routing key: {}", routingKey)
            }
        }
        log.info("Started ordering updates consumer, tag={}", consumerTag)
        return consumerTag
    }

    private fun applyAndLog(orderId: String, target: OrderStatus) {
        when (val r = orderRepository.applyLifecycleStatus(orderId, target)) {
            ApplyLifecycleStatusResult.Applied -> log.debug("Applied {} -> {}", orderId, target)
            ApplyLifecycleStatusResult.SkippedNoop ->
                log.debug("Skipped lifecycle apply (noop/stale) orderId={} target={}", orderId, target)
            ApplyLifecycleStatusResult.NotFound ->
                log.warn("Lifecycle event for unknown orderId={} target={}", orderId, target)
        }
    }
}
