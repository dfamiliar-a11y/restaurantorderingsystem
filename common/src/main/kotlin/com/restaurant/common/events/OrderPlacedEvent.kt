package com.restaurant.common.events

import kotlinx.serialization.Serializable

/**
 * Published by Ordering when a new order is persisted with status [OrderStatus.PLACED].
 *
 * This is the **outbound domain event** that kicks off asynchronous processing: Kitchen
 * consumes messages with routing key [RestaurantRabbitTopology.ROUTING_ORDER_NEW] and reacts by emitting
 * [OrderPreparingEvent] back onto the same topic exchange. Staff later confirm readiness via
 * ordering-api HTTP, which publishes [DishPreparedEvent] (`dish.ready`).
 */
@Serializable
data class OrderPlacedEvent(
    val orderId: String,
    val tableNumber: Int,
    val lines: List<OrderLineEvent>,
)
