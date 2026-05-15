package com.restaurant.common

import com.rabbitmq.client.AMQP

/**
 * AMQP message properties for JSON event payloads published to durable queues.
 *
 * **Persistence:** `deliveryMode = 2` (persistent) is required for messages to survive broker
 * restarts when queues and exchanges are durable—without it, only the topology survives, not
 * queued message bodies.
 */
fun jsonPersistentMessageProperties(): AMQP.BasicProperties =
    AMQP.BasicProperties.Builder()
        .deliveryMode(2)
        .contentType("application/json")
        .build()
