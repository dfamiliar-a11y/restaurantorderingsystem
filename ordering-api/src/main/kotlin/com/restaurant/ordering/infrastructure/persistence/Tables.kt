package com.restaurant.ordering.infrastructure.persistence

import com.restaurant.common.OrderStatus
import org.jetbrains.exposed.sql.ReferenceOption
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.javatime.timestamp
import java.time.Instant

/** Header row for an order aggregate (status is the projection updated by HTTP placement and async consumers). */
object OrdersTable : Table("orders") {
    val id = varchar("id", 36)
    val tableNumber = integer("table_number")
    val status = varchar("status", 32)
    val createdAt = timestamp("created_at")

    override val primaryKey = PrimaryKey(id)
}

/**
 * Child lines for an order; [orderId] references [OrdersTable] with cascade delete so the aggregate
 * cannot orphan rows when removed.
 */
object OrderItemsTable : Table("order_items") {
    val id = varchar("id", 36)
    val orderId = varchar("order_id", 36).references(OrdersTable.id, onDelete = ReferenceOption.CASCADE)
    val menuItemId = varchar("menu_item_id", 64)
    val name = varchar("name", 255)
    val quantity = integer("quantity")
    val unitPrice = double("unit_price")

    override val primaryKey = PrimaryKey(id)
}

/** Maps domain enum to the persisted varchar representation (stable `name` values). */
fun OrderStatus.toColumn(): String = name

/** Restores [OrderStatus] from column storage; invalid values surface as deserialization-time failures. */
fun String.toOrderStatus(): OrderStatus = OrderStatus.valueOf(this)

/** Centralized clock source for `created_at` to keep tests and production behavior aligned. */
fun newCreatedAt(): Instant = Instant.now()
