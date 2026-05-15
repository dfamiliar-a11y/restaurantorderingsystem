package com.restaurant.ordering.infrastructure.messaging

import com.rabbitmq.client.Channel
import com.restaurant.common.events.DishPreparedEvent
import com.restaurant.common.events.OrderPlacedEvent
import com.restaurant.common.jsonPersistentMessageProperties
import com.restaurant.common.RestaurantRabbitTopology
import com.restaurant.ordering.domain.OrderEventPublisher
import kotlinx.serialization.json.Json

private const val CONFIRM_TIMEOUT_MS = 5_000L

/**
 * **Messaging adapter** implementing [OrderEventPublisher]: encodes events as JSON and publishes
 * to the configured topic exchange (`order.new`, `dish.ready`).
 *
 * **Thread safety:** the Java client's [Channel] is not thread-safe; all use of this channel is
 * guarded by `synchronized(channel)` together with [waitForConfirms][com.rabbitmq.client.Channel.waitForConfirms]
 * so concurrent Ktor worker threads cannot interleave AMQP frames.
 *
 * **Reliability:** [publisher confirms][com.rabbitmq.client.Channel.confirmSelect] plus persistent
 * [message properties][com.restaurant.common.jsonPersistentMessageProperties] improve durability
 * visibility; strict exactly-once to the broker still needs an **outbox** (see class KDoc on
 * [PublishNotAcknowledgedException]).
 */
class RabbitOrderEventPublisher(
    private val channel: Channel,
    /** Must match [RestaurantRabbitTopology.declareOrdersExchange] and HOCON `restaurant.rabbitMq.exchangeName`. */
    private val ordersExchangeName: String,
    private val json: Json = Json { ignoreUnknownKeys = true },
) : OrderEventPublisher {
    init {
        synchronized(channel) {
            channel.confirmSelect()
        }
    }

    override fun publishOrderPlaced(event: OrderPlacedEvent) {
        val body = json.encodeToString(event).toByteArray(Charsets.UTF_8)
        synchronized(channel) {
            channel.basicPublish(
                ordersExchangeName,
                RestaurantRabbitTopology.ROUTING_ORDER_NEW,
                jsonPersistentMessageProperties(),
                body,
            )
            if (!channel.waitForConfirms(CONFIRM_TIMEOUT_MS)) {
                throw PublishNotAcknowledgedException(
                    "Broker did not ack order.new publish within ${CONFIRM_TIMEOUT_MS}ms",
                )
            }
        }
    }

    override fun publishDishPrepared(event: DishPreparedEvent) {
        val body = json.encodeToString(event).toByteArray(Charsets.UTF_8)
        synchronized(channel) {
            channel.basicPublish(
                ordersExchangeName,
                RestaurantRabbitTopology.ROUTING_DISH_READY,
                jsonPersistentMessageProperties(),
                body,
            )
            if (!channel.waitForConfirms(CONFIRM_TIMEOUT_MS)) {
                throw PublishNotAcknowledgedException(
                    "Broker did not ack dish.ready publish within ${CONFIRM_TIMEOUT_MS}ms",
                )
            }
        }
    }
}
