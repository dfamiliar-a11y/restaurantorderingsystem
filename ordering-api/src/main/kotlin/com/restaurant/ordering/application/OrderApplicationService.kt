package com.restaurant.ordering.application

import com.restaurant.common.Menu
import com.restaurant.common.events.DishPreparedEvent
import com.restaurant.common.events.OrderLineEvent
import com.restaurant.common.events.OrderPlacedEvent
import com.restaurant.common.OrderStatus
import com.restaurant.ordering.domain.OrderEventPublisher
import com.restaurant.ordering.domain.OrderLine
import com.restaurant.ordering.domain.OrderRepository

/** Outcome of [OrderApplicationService.markReadyToServe] for HTTP mapping. */
sealed interface MarkReadyToServeResult {
    data object Published : MarkReadyToServeResult
    /** Idempotent: order already `READY_TO_SERVE`; no duplicate publish. */
    data object AlreadyReady : MarkReadyToServeResult
    data object OrderNotFound : MarkReadyToServeResult
    data class InvalidStatus(val status: OrderStatus) : MarkReadyToServeResult
}

/**
 * Application / use-case layer for Ordering: orchestrates domain validation, persistence, and
 * outbound notifications without depending on Ktor or RabbitMQ types.
 *
 * [placeOrder] follows a **write-then-publish** sequence: the aggregate is persisted first so
 * downstream consumers never observe an event for an order that failed to save; the
 * [OrderPlacedEvent] then triggers asynchronous Kitchen work via the injected [OrderEventPublisher] port.
 *
 * **Broker reliability:** the Rabbit adapter uses **publisher confirms** and persistent messages. If
 * the broker does not ack a publish, [PublishNotAcknowledgedException] propagates and Ktor maps it to
 * **503**—the row may already exist (**dual-write**). A production deployment would add an **outbox**
 * or idempotent client retries; that is intentionally out of scope for this demo.
 */
class OrderApplicationService(
    private val orderRepository: OrderRepository,
    private val orderEventPublisher: OrderEventPublisher,
) {
    /** Returns the static in-memory catalog shared across services for demo purposes. */
    fun getMenu() = Menu.items

    /**
     * Validates table and line items against [Menu], persists a new order in [OrderStatus.PLACED],
     * and publishes [OrderPlacedEvent] for Kitchen to pick up.
     *
     * @return server-generated order identifier
     */
    fun placeOrder(tableNumber: Int, requestedItems: List<Pair<String, Int>>): String {
        require(tableNumber > 0) { "tableNumber must be positive" }
        require(requestedItems.isNotEmpty()) { "items must not be empty" }

        val lines = requestedItems.map { (menuItemId, quantity) ->
            require(quantity > 0) { "quantity must be positive for $menuItemId" }
            val item = requireNotNull(Menu.findById(menuItemId)) { "Unknown menu item: $menuItemId" }
            OrderLine(
                menuItemId = item.id,
                name = item.name,
                quantity = quantity,
                unitPrice = item.price,
            )
        }

        val orderId = orderRepository.createOrder(tableNumber, lines)

        val event = OrderPlacedEvent(
            orderId = orderId,
            tableNumber = tableNumber,
            lines = lines.map {
                OrderLineEvent(
                    menuItemId = it.menuItemId,
                    name = it.name,
                    quantity = it.quantity,
                    unitPrice = it.unitPrice,
                )
            },
        )
        orderEventPublisher.publishOrderPlaced(event)
        return orderId
    }

    /**
     * Staff / expediter confirms the order is on the pass: publishes [DishPreparedEvent] (`dish.ready`)
     * when status is [OrderStatus.PREPARING]. Repeating the call when already [OrderStatus.READY_TO_SERVE]
     * is a no-op (no second publish). Other statuses return [MarkReadyToServeResult.InvalidStatus].
     */
    fun markReadyToServe(orderId: String): MarkReadyToServeResult {
        val order = orderRepository.findOrder(orderId) ?: return MarkReadyToServeResult.OrderNotFound
        return when (order.status) {
            OrderStatus.PREPARING -> {
                val summary = order.items.joinToString(", ") { "${it.quantity}x ${it.name}" }
                val ready = DishPreparedEvent(
                    orderId = order.id,
                    tableNumber = order.tableNumber,
                    dishSummary = summary,
                )
                orderEventPublisher.publishDishPrepared(ready)
                MarkReadyToServeResult.Published
            }
            OrderStatus.READY_TO_SERVE -> MarkReadyToServeResult.AlreadyReady
            else -> MarkReadyToServeResult.InvalidStatus(order.status)
        }
    }

    /** Read-only view of persisted state (including status updated asynchronously by messaging). */
    fun getOrder(orderId: String) = orderRepository.findOrder(orderId)
}
