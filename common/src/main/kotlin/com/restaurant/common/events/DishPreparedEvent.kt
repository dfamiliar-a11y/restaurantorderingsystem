package com.restaurant.common.events

import kotlinx.serialization.Serializable

/**
 * Emitted when staff confirm the order is ready for front-of-house / delivery handoff
 * ([OrderApplicationService.markReadyToServe] over HTTP).
 *
 * Runner consumes [ROUTING_DISH_READY][RestaurantRabbitTopology.ROUTING_DISH_READY]; Ordering listens on
 * the same exchange to set [OrderStatus.READY_TO_SERVE] for API clients polling `GET /order/{id}`.
 */
@Serializable
data class DishPreparedEvent(
    val orderId: String,
    val tableNumber: Int,
    val dishSummary: String,
)
