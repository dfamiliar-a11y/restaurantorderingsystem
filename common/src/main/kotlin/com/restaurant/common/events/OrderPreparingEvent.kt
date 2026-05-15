package com.restaurant.common.events

import kotlinx.serialization.Serializable

/**
 * Emitted by Kitchen after it has accepted work for an order (kitchen queue / ticket intake).
 *
 * Ordering's [com.restaurant.ordering.infrastructure.messaging.OrderingUpdatesConsumer] binds
 * this routing key so the read model in the database advances to [com.restaurant.common.OrderStatus.PREPARING].
 */
@Serializable
data class OrderPreparingEvent(
    val orderId: String,
)
