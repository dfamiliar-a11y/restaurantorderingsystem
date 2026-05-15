package com.restaurant.ordering.infrastructure.web

import com.restaurant.common.OrderStatus
import kotlinx.serialization.Serializable

/**
 * JSON contracts for the Ordering HTTP API (distinct from [com.restaurant.common] event DTOs
 * so transport shapes can evolve separately from broker payloads).
 */
@Serializable
data class PlaceOrderItemRequest(
    val menuItemId: String,
    val quantity: Int,
)

@Serializable
data class PlaceOrderRequest(
    val tableNumber: Int,
    val items: List<PlaceOrderItemRequest>,
)

@Serializable
data class PlaceOrderResponse(
    val orderId: String,
)

@Serializable
data class OrderItemResponse(
    val menuItemId: String,
    val name: String,
    val quantity: Int,
    val unitPrice: Double,
)

@Serializable
data class GetOrderResponse(
    val orderId: String,
    val tableNumber: Int,
    val status: OrderStatus,
    val items: List<OrderItemResponse>,
)

@Serializable
data class ErrorResponse(
    val error: String,
)
