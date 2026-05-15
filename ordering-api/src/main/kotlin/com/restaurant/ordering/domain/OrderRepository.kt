package com.restaurant.ordering.domain

import com.restaurant.common.OrderStatus

/**
 * Outcome of [OrderRepository.applyLifecycleStatus]: used by the Rabbit consumer so **at-least-once**
 * redeliveries and out-of-order edges never move an order **backwards** in the lifecycle.
 */
enum class ApplyLifecycleStatusResult {
    /** Row existed, current status was in the allowed predecessor set, and is now [target]. */
    Applied,

    /** Order row missing (unknown id or race with retention). */
    NotFound,

    /**
     * No row updated because the aggregate was already at or past the requested state (duplicate /
     * stale event). Still a **success** from a messaging perspective: ack the delivery.
     */
    SkippedNoop,
}

/**
 * **Persistence port** for the order aggregate: hides Exposed/SQL details behind stable application operations.
 *
 * Status transitions driven by messaging use [applyLifecycleStatus] so Rabbit redeliveries stay idempotent.
 */
interface OrderRepository {
    /** Inserts header and lines; initial status is expected to be [OrderStatus.PLACED] in the adapter. */
    fun createOrder(tableNumber: Int, lines: List<OrderLine>): String

    /** Loads aggregate header and lines, or `null` if the identifier is unknown. */
    fun findOrder(orderId: String): OrderRecord?

    /**
     * @return `true` if a row was updated, `false` if [orderId] was not found
     */
    fun updateStatus(orderId: String, status: OrderStatus): Boolean

    /**
     * Applies a **monotonic** lifecycle transition from Kitchen/Runner facts: updates only when the
     * current row status is one of the allowed predecessors for [target] (including already at
     * [target] for idempotent replays). See [ApplyLifecycleStatusResult].
     */
    fun applyLifecycleStatus(orderId: String, target: OrderStatus): ApplyLifecycleStatusResult
}
