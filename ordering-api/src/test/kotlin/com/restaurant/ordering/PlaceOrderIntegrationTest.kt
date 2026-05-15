package com.restaurant.ordering

import com.restaurant.common.events.DishPreparedEvent
import com.restaurant.common.events.OrderPlacedEvent
import com.restaurant.common.OrderStatus
import com.restaurant.ordering.application.OrderApplicationService
import com.restaurant.ordering.domain.OrderEventPublisher
import com.restaurant.ordering.infrastructure.persistence.ExposedOrderRepository
import com.restaurant.ordering.infrastructure.persistence.OrderItemsTable
import com.restaurant.ordering.infrastructure.persistence.OrdersTable
import com.restaurant.ordering.infrastructure.web.ErrorResponse
import com.restaurant.ordering.infrastructure.web.PlaceOrderItemRequest
import com.restaurant.ordering.infrastructure.web.PlaceOrderRequest
import com.restaurant.ordering.infrastructure.web.PlaceOrderResponse
import com.restaurant.ordering.infrastructure.web.configureHttp
import io.ktor.client.call.body
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.testing.testApplication
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID

private class RecordingOrderEventPublisher : OrderEventPublisher {
    val placed = mutableListOf<OrderPlacedEvent>()
    val dishPrepared = mutableListOf<DishPreparedEvent>()

    override fun publishOrderPlaced(event: OrderPlacedEvent) {
        placed.add(event)
    }

    override fun publishDishPrepared(event: DishPreparedEvent) {
        dishPrepared.add(event)
    }
}

class PlaceOrderIntegrationTest {

    @Test
    fun `post order persists PLACED and publishes OrderPlacedEvent`() = runBlocking {
        val jdbcUrl = "jdbc:h2:mem:it_${UUID.randomUUID()};DB_CLOSE_DELAY=-1;MODE=PostgreSQL"
        Database.connect(jdbcUrl, driver = "org.h2.Driver")
        transaction {
            SchemaUtils.create(OrdersTable, OrderItemsTable)
        }

        val repository = ExposedOrderRepository()
        val publisher = RecordingOrderEventPublisher()
        val service = OrderApplicationService(repository, publisher)

        testApplication {
            application {
                configureHttp(service)
            }
            val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }
            val client = createClient {
                install(ContentNegotiation) {
                    json(json)
                }
            }

            val response = client.post("/order") {
                contentType(ContentType.Application.Json)
                setBody(
                    PlaceOrderRequest(
                        tableNumber = 7,
                        items = listOf(
                            PlaceOrderItemRequest(menuItemId = "caesar_salad", quantity = 2),
                            PlaceOrderItemRequest(menuItemId = "craft_beer", quantity = 1),
                        ),
                    ),
                )
            }

            assertEquals(HttpStatusCode.Created, response.status)
            val body = response.body<PlaceOrderResponse>()
            assertNotNull(body.orderId)

            val stored = repository.findOrder(body.orderId)
            assertNotNull(stored)
            assertEquals(OrderStatus.PLACED, stored!!.status)
            assertEquals(7, stored.tableNumber)

            assertEquals(1, publisher.placed.size)
            val event = publisher.placed.single()
            assertEquals(body.orderId, event.orderId)
            assertEquals(7, event.tableNumber)
            assertEquals(2, event.lines.size)
            assertEquals("caesar_salad", event.lines[0].menuItemId)
            assertEquals(2, event.lines[0].quantity)
            assertEquals("craft_beer", event.lines[1].menuItemId)
        }
    }

    @Test
    fun `post ready to serve when preparing publishes DishPreparedEvent and returns 204`() = runBlocking {
        val jdbcUrl = "jdbc:h2:mem:it_${UUID.randomUUID()};DB_CLOSE_DELAY=-1;MODE=PostgreSQL"
        Database.connect(jdbcUrl, driver = "org.h2.Driver")
        transaction {
            SchemaUtils.create(OrdersTable, OrderItemsTable)
        }

        val repository = ExposedOrderRepository()
        val publisher = RecordingOrderEventPublisher()
        val service = OrderApplicationService(repository, publisher)

        testApplication {
            application {
                configureHttp(service)
            }
            val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }
            val client = createClient {
                install(ContentNegotiation) {
                    json(json)
                }
            }

            val orderId = service.placeOrder(
                3,
                listOf("caesar_salad" to 1),
            )
            repository.updateStatus(orderId, OrderStatus.PREPARING)

            val response = client.post("/order/$orderId/ready-to-serve")

            assertEquals(HttpStatusCode.NoContent, response.status)
            assertEquals(1, publisher.dishPrepared.size)
            val dish = publisher.dishPrepared.single()
            assertEquals(orderId, dish.orderId)
            assertEquals(3, dish.tableNumber)
            assertTrue(dish.dishSummary.contains("Caesar Salad", ignoreCase = true))
        }
    }

    @Test
    fun `post ready to serve returns 404 when order unknown`() = runBlocking {
        val jdbcUrl = "jdbc:h2:mem:it_${UUID.randomUUID()};DB_CLOSE_DELAY=-1;MODE=PostgreSQL"
        Database.connect(jdbcUrl, driver = "org.h2.Driver")
        transaction {
            SchemaUtils.create(OrdersTable, OrderItemsTable)
        }

        val repository = ExposedOrderRepository()
        val publisher = RecordingOrderEventPublisher()
        val service = OrderApplicationService(repository, publisher)

        testApplication {
            application {
                configureHttp(service)
            }
            val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }
            val client = createClient {
                install(ContentNegotiation) {
                    json(json)
                }
            }

            val response = client.post("/order/${UUID.randomUUID()}/ready-to-serve")
            assertEquals(HttpStatusCode.NotFound, response.status)
            assertEquals("order not found", response.body<ErrorResponse>().error)
            assertTrue(publisher.dishPrepared.isEmpty())
        }
    }

    @Test
    fun `post ready to serve returns 409 when order still placed`() = runBlocking {
        val jdbcUrl = "jdbc:h2:mem:it_${UUID.randomUUID()};DB_CLOSE_DELAY=-1;MODE=PostgreSQL"
        Database.connect(jdbcUrl, driver = "org.h2.Driver")
        transaction {
            SchemaUtils.create(OrdersTable, OrderItemsTable)
        }

        val repository = ExposedOrderRepository()
        val publisher = RecordingOrderEventPublisher()
        val service = OrderApplicationService(repository, publisher)

        testApplication {
            application {
                configureHttp(service)
            }
            val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }
            val client = createClient {
                install(ContentNegotiation) {
                    json(json)
                }
            }

            val orderId = service.placeOrder(1, listOf("beef_burger" to 1))
            val response = client.post("/order/$orderId/ready-to-serve")
            assertEquals(HttpStatusCode.Conflict, response.status)
            assertTrue(response.body<ErrorResponse>().error.contains("PLACED"))
            assertTrue(publisher.dishPrepared.isEmpty())
        }
    }

    @Test
    fun `post ready to serve is idempotent when already ready to serve`() = runBlocking {
        val jdbcUrl = "jdbc:h2:mem:it_${UUID.randomUUID()};DB_CLOSE_DELAY=-1;MODE=PostgreSQL"
        Database.connect(jdbcUrl, driver = "org.h2.Driver")
        transaction {
            SchemaUtils.create(OrdersTable, OrderItemsTable)
        }

        val repository = ExposedOrderRepository()
        val publisher = RecordingOrderEventPublisher()
        val service = OrderApplicationService(repository, publisher)

        testApplication {
            application {
                configureHttp(service)
            }
            val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }
            val client = createClient {
                install(ContentNegotiation) {
                    json(json)
                }
            }

            val orderId = service.placeOrder(2, listOf("margherita_pizza" to 1))
            repository.updateStatus(orderId, OrderStatus.PREPARING)

            assertEquals(HttpStatusCode.NoContent, client.post("/order/$orderId/ready-to-serve").status)
            assertEquals(1, publisher.dishPrepared.size)

            repository.updateStatus(orderId, OrderStatus.READY_TO_SERVE)

            assertEquals(HttpStatusCode.NoContent, client.post("/order/$orderId/ready-to-serve").status)
            assertEquals(1, publisher.dishPrepared.size)
        }
    }
}
