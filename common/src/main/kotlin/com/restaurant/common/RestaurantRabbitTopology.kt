package com.restaurant.common

import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.Channel

/**
 * Central **RabbitMQ topology** for the restaurant demo: one durable topic exchange for business
 * events, a small **dead-letter exchange (DLX)** for poison or rejected messages, and
 * service-specific queues bound by routing key.
 *
 * **Exchange name:** pass [com.restaurant.common.config.AppConfig.rabbitMq] `.exchangeName`
 * (HOCON `restaurant.rabbitMq.exchangeName` / env `RABBITMQ_EXCHANGE`) into each `declare*` method
 * so `exchangeDeclare` and `basicPublish` use the same string. [EXCHANGE_ORDERS_DEFAULT] matches
 * the default in [application.conf](classpath:application.conf).
 *
 * **Dead-letter:** each primary queue sets `x-dead-letter-exchange` / `x-dead-letter-routing-key`
 * toward [DLX_NAME]. When a consumer [negative-acks without requeue][com.rabbitmq.client.Channel.basicNack],
 * the broker republishes to the DLX and messages land in the corresponding `*.dlq` queue.
 */
object RestaurantRabbitTopology {

    /** Default topic exchange name; aligned with `restaurant.rabbitMq.exchangeName` in HOCON. */
    const val EXCHANGE_ORDERS_DEFAULT = "restaurant.orders"

    /** Direct exchange used only to route rejected messages into DLQ queues. */
    const val DLX_NAME = "restaurant.orders.dlx"

    const val QUEUE_KITCHEN = "kitchen.orders"
    const val QUEUE_RUNNER = "runner.delivery"
    const val QUEUE_ORDERING_UPDATES = "ordering.updates"

    const val QUEUE_KITCHEN_DLQ = "kitchen.orders.dlq"
    const val QUEUE_RUNNER_DLQ = "runner.delivery.dlq"
    const val QUEUE_ORDERING_UPDATES_DLQ = "ordering.updates.dlq"

    const val ROUTING_DLQ_KITCHEN = "dlq.kitchen.orders"
    const val ROUTING_DLQ_RUNNER = "dlq.runner.delivery"
    const val ROUTING_DLQ_ORDERING_UPDATES = "dlq.ordering.updates"

    const val ROUTING_ORDER_NEW = "order.new"
    const val ROUTING_ORDER_PREPARING = "order.preparing"
    const val ROUTING_DISH_READY = "dish.ready"
    const val ROUTING_DELIVERY_CONFIRMED = "delivery.confirmed"

    private fun declareDlxAndDlqs(channel: Channel) {
        channel.exchangeDeclare(DLX_NAME, BuiltinExchangeType.DIRECT, true)
        channel.queueDeclare(QUEUE_ORDERING_UPDATES_DLQ, true, false, false, emptyMap())
        channel.queueBind(QUEUE_ORDERING_UPDATES_DLQ, DLX_NAME, ROUTING_DLQ_ORDERING_UPDATES)
        channel.queueDeclare(QUEUE_KITCHEN_DLQ, true, false, false, emptyMap())
        channel.queueBind(QUEUE_KITCHEN_DLQ, DLX_NAME, ROUTING_DLQ_KITCHEN)
        channel.queueDeclare(QUEUE_RUNNER_DLQ, true, false, false, emptyMap())
        channel.queueBind(QUEUE_RUNNER_DLQ, DLX_NAME, ROUTING_DLQ_RUNNER)
    }

    private fun dlqArgs(routingKeyToDlx: String): Map<String, Any> =
        mapOf(
            "x-dead-letter-exchange" to DLX_NAME,
            "x-dead-letter-routing-key" to routingKeyToDlx,
        )

    /** Declares the shared topic exchange for order lifecycle events (idempotent at broker). */
    fun declareOrdersExchange(channel: Channel, ordersExchangeName: String) {
        channel.exchangeDeclare(ordersExchangeName, BuiltinExchangeType.TOPIC, true)
    }

    /** Kitchen consumes new orders placed by Ordering. */
    fun declareKitchenTopology(channel: Channel, ordersExchangeName: String) {
        declareDlxAndDlqs(channel)
        declareOrdersExchange(channel, ordersExchangeName)
        channel.queueDeclare(QUEUE_KITCHEN, true, false, false, dlqArgs(ROUTING_DLQ_KITCHEN))
        channel.queueBind(QUEUE_KITCHEN, ordersExchangeName, ROUTING_ORDER_NEW)
    }

    /** Runner consumes "ready to serve" handoff events. */
    fun declareRunnerTopology(channel: Channel, ordersExchangeName: String) {
        declareDlxAndDlqs(channel)
        declareOrdersExchange(channel, ordersExchangeName)
        channel.queueDeclare(QUEUE_RUNNER, true, false, false, dlqArgs(ROUTING_DLQ_RUNNER))
        channel.queueBind(QUEUE_RUNNER, ordersExchangeName, ROUTING_DISH_READY)
    }

    /**
     * Ordering listens for all post-placement lifecycle keys so one consumer can advance status
     * from Kitchen and Runner without separate exchanges.
     */
    fun declareOrderingConsumerTopology(channel: Channel, ordersExchangeName: String) {
        declareDlxAndDlqs(channel)
        declareOrdersExchange(channel, ordersExchangeName)
        channel.queueDeclare(QUEUE_ORDERING_UPDATES, true, false, false, dlqArgs(ROUTING_DLQ_ORDERING_UPDATES))
        channel.queueBind(QUEUE_ORDERING_UPDATES, ordersExchangeName, ROUTING_ORDER_PREPARING)
        channel.queueBind(QUEUE_ORDERING_UPDATES, ordersExchangeName, ROUTING_DISH_READY)
        channel.queueBind(QUEUE_ORDERING_UPDATES, ordersExchangeName, ROUTING_DELIVERY_CONFIRMED)
    }
}
