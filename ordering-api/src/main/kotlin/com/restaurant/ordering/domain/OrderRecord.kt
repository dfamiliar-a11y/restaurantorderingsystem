package com.restaurant.ordering.domain

import com.restaurant.common.OrderStatus

/**
 * Read model for the order aggregate returned from [OrderRepository.findOrder].
 * Split from [OrderRepository] so the persistence port stays a single-responsibility file.
 */
data class OrderRecord(
    val id: String,
    val tableNumber: Int,
    val status: OrderStatus,
    val items: List<OrderLine>,
)
