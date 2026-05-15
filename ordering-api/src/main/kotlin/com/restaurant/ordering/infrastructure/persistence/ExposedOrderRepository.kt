package com.restaurant.ordering.infrastructure.persistence

import com.restaurant.common.OrderStatus
import com.restaurant.ordering.domain.ApplyLifecycleStatusResult
import com.restaurant.ordering.domain.OrderLine
import com.restaurant.ordering.domain.OrderRecord
import com.restaurant.ordering.domain.OrderRepository
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.update
import java.util.UUID

/**
 * Exposed-backed adapter for [OrderRepository]: one transaction per public call keeps consistency
 * between `orders` and `order_items` without leaking SQL types upward.
 */
class ExposedOrderRepository : OrderRepository {
    override fun createOrder(tableNumber: Int, lines: List<OrderLine>): String = transaction {
        val orderId = UUID.randomUUID().toString()
        OrdersTable.insert {
            it[id] = orderId
            it[OrdersTable.tableNumber] = tableNumber
            it[status] = OrderStatus.PLACED.toColumn()
            it[createdAt] = newCreatedAt()
        }
        lines.forEach { line ->
            OrderItemsTable.insert {
                it[id] = UUID.randomUUID().toString()
                it[OrderItemsTable.orderId] = orderId
                it[menuItemId] = line.menuItemId
                it[name] = line.name
                it[quantity] = line.quantity
                it[unitPrice] = line.unitPrice
            }
        }
        orderId
    }

    override fun findOrder(orderId: String): OrderRecord? = transaction {
        val orderRow = OrdersTable.selectAll().where { OrdersTable.id eq orderId }.singleOrNull()
            ?: return@transaction null
        val itemRows = OrderItemsTable.selectAll().where { OrderItemsTable.orderId eq orderId }
        OrderRecord(
            id = orderRow[OrdersTable.id],
            tableNumber = orderRow[OrdersTable.tableNumber],
            status = orderRow[OrdersTable.status].toOrderStatus(),
            items = itemRows.map { row ->
                OrderLine(
                    menuItemId = row[OrderItemsTable.menuItemId],
                    name = row[OrderItemsTable.name],
                    quantity = row[OrderItemsTable.quantity],
                    unitPrice = row[OrderItemsTable.unitPrice],
                )
            },
        )
    }

    override fun updateStatus(orderId: String, status: OrderStatus): Boolean = transaction {
        val updated = OrdersTable.update({ OrdersTable.id eq orderId }) {
            it[OrdersTable.status] = status.toColumn()
        }
        updated > 0
    }

    override fun applyLifecycleStatus(orderId: String, target: OrderStatus): ApplyLifecycleStatusResult =
        transaction {
            val allowedCurrent: List<OrderStatus> =
                when (target) {
                    OrderStatus.PREPARING -> listOf(OrderStatus.PLACED, OrderStatus.PREPARING)
                    OrderStatus.READY_TO_SERVE -> listOf(OrderStatus.PREPARING, OrderStatus.READY_TO_SERVE)
                    OrderStatus.SERVED -> listOf(OrderStatus.READY_TO_SERVE, OrderStatus.SERVED)
                    OrderStatus.PLACED -> emptyList()
                }
            if (allowedCurrent.isEmpty()) {
                return@transaction ApplyLifecycleStatusResult.SkippedNoop
            }
            val allowedColumns = allowedCurrent.map { it.toColumn() }
            val updated =
                OrdersTable.update({
                    OrdersTable.id eq orderId and (OrdersTable.status inList allowedColumns)
                }) {
                    it[OrdersTable.status] = target.toColumn()
                }
            when {
                updated > 0 -> ApplyLifecycleStatusResult.Applied
                else -> {
                    val exists = OrdersTable.selectAll().where { OrdersTable.id eq orderId }.any()
                    if (exists) ApplyLifecycleStatusResult.SkippedNoop
                    else ApplyLifecycleStatusResult.NotFound
                }
            }
        }
}
