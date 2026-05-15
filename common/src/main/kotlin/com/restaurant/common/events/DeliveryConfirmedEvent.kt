package com.restaurant.common.events

import kotlinx.serialization.Serializable

/**
 * Emitted by Runner after simulated delivery to the table.
 *
 * Ordering updates persistence to [com.restaurant.common.OrderStatus.SERVED], completing the lifecycle visible over HTTP.
 */
@Serializable
data class DeliveryConfirmedEvent(
    val orderId: String,
)
