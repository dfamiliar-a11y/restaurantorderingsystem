package com.restaurant.ordering.domain

import com.restaurant.common.events.DishPreparedEvent
import com.restaurant.common.events.OrderPlacedEvent

/**
 * **Outbound port** (hexagonal / clean architecture): application code announces order lifecycle
 * facts without knowing whether delivery is RabbitMQ, an in-memory test double, or another broker.
 *
 * The production adapter is [com.restaurant.ordering.infrastructure.messaging.RabbitOrderEventPublisher],
 * which publishes to `restaurant.orders` with the appropriate routing keys.
 */
interface OrderEventPublisher {
    /** Publishes a durable fact that a new aggregate instance exists and is ready for downstream processing. */
    fun publishOrderPlaced(event: OrderPlacedEvent)

    /** Publishes `dish.ready` when staff confirms the order is ready for handoff (runner / front of house). */
    fun publishDishPrepared(event: DishPreparedEvent)
}
