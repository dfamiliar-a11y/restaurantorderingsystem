package com.restaurant.common.events

import kotlinx.serialization.Serializable

/**
 * Wire-format line item for [OrderPlacedEvent] on the shared orders exchange (`com.restaurant.common.events`).
 *
 * Serializable line item carried on the wire inside [OrderPlacedEvent]. Mirrors menu selection and pricing
 * at the time the order was placed so downstream services (for example Kitchen) can work from a stable
 * snapshot without re-querying the catalog.
 */
@Serializable
data class OrderLineEvent(
    val menuItemId: String,
    val name: String,
    val quantity: Int,
    val unitPrice: Double,
)
