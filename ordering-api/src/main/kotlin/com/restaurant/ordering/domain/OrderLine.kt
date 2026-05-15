package com.restaurant.ordering.domain

/**
 * Domain line item for the order aggregate: persisted with the order and used when building
 * outbound events. Lives alongside [OrderRecord] and [OrderRepository] under `domain`.
 */
data class OrderLine(
    val menuItemId: String,
    val name: String,
    val quantity: Int,
    val unitPrice: Double,
)
