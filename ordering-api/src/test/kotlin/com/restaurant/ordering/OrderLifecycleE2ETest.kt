package com.restaurant.ordering

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import com.restaurant.common.events.DeliveryConfirmedEvent
import com.restaurant.common.events.DishPreparedEvent
import com.restaurant.common.events.OrderPlacedEvent
import com.restaurant.common.events.OrderPreparingEvent
import com.restaurant.common.OrderStatus
import com.restaurant.common.consumeManualAck
import com.restaurant.common.jsonPersistentMessageProperties
import com.restaurant.common.RestaurantRabbitTopology
import com.restaurant.ordering.application.OrderApplicationService
import com.restaurant.ordering.infrastructure.messaging.OrderingUpdatesConsumer
import com.restaurant.ordering.infrastructure.messaging.RabbitOrderEventPublisher
import com.restaurant.ordering.infrastructure.persistence.ExposedOrderRepository
import com.restaurant.ordering.infrastructure.persistence.OrderItemsTable
import com.restaurant.ordering.infrastructure.persistence.OrdersTable
import com.restaurant.ordering.infrastructure.web.GetOrderResponse
import com.restaurant.ordering.infrastructure.web.PlaceOrderItemRequest
import com.restaurant.ordering.infrastructure.web.PlaceOrderRequest
import com.restaurant.ordering.infrastructure.web.PlaceOrderResponse
import com.restaurant.ordering.infrastructure.web.configureHttp
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.netty.NettyApplicationEngine
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.util.UUID
import java.util.concurrent.CountDownLatch
import kotlin.concurrent.thread

/**
 * Backend E2E: HTTP place order → RabbitMQ → kitchen (`order.preparing`) → staff POST ready-to-serve
 * → `dish.ready` → runner → ordering consumer updates status → GET /order/{id} ends at [OrderStatus.SERVED].
 */
@Testcontainers
class OrderLifecycleE2ETest {

    companion object {
        /**
         * Shared RabbitMQ broker for all tests in this class.
         *
         * Testcontainers starts one container per class (not per method) to amortize Docker pull/start.
         * `@JvmField` keeps JUnit 5 field injection happy for the `@Container` annotation.
         */
        @Container
        @JvmField
        val rabbit: RabbitMQContainer = RabbitMQContainer(DockerImageName.parse("rabbitmq:3.13-alpine"))
    }

    /** Matches production JSON settings for event DTOs and HTTP bodies. */
    private val json = Json {
        ignoreUnknownKeys = true
        encodeDefaults = true
    }

    /** Rabbit connection factory aimed at the Testcontainers-mapped AMQP port. */
    private lateinit var factory: ConnectionFactory

    /** Ordering-api side: publish [OrderPlacedEvent] / [DishPreparedEvent] and run [OrderingUpdatesConsumer]. */
    private lateinit var apiConnection: Connection
    private lateinit var publishChannel: Channel
    private lateinit var consumerChannel: Channel

    /** Persistence + application service passed into [configureHttp]. */
    private lateinit var repository: ExposedOrderRepository
    private lateinit var service: OrderApplicationService

    /** Embedded HTTP server; [httpPort] resolved after [EmbeddedServer.start]. */
    private lateinit var server: EmbeddedServer<NettyApplicationEngine, NettyApplicationEngine.Configuration>
    private lateinit var kitchen: KitchenSimulator
    private lateinit var runner: RunnerSimulator
    private var httpPort: Int = -1

    private val ordersExchange = RestaurantRabbitTopology.EXCHANGE_ORDERS_DEFAULT

    /**
     * Boots the full harness in dependency order: broker topology → database → messaging → HTTP.
     *
     * Ordering matters: [OrderingUpdatesConsumer] must be consuming before simulators publish, and
     * simulators must be listening before the test issues `POST /order`.
     */
    @BeforeEach
    fun setup() {
        factory = ConnectionFactory().apply {
            host = rabbit.host
            port = rabbit.getMappedPort(5672)
            username = "guest"
            password = "guest"
            virtualHost = "/"
        }

        // Idempotent declare on a throwaway connection: ensures kitchen/runner/ordering queues exist
        // before any long-lived consumer attaches (mirrors each service declaring on startup).
        factory.newConnection().use { conn ->
            conn.createChannel().use { ch ->
                RestaurantRabbitTopology.declareKitchenTopology(ch, ordersExchange)
                RestaurantRabbitTopology.declareRunnerTopology(ch, ordersExchange)
                RestaurantRabbitTopology.declareOrderingConsumerTopology(ch, ordersExchange)
            }
        }

        // Per-test H2 database — UUID in URL prevents cross-test leakage when Exposed reuses static state.
        val jdbcUrl = "jdbc:h2:mem:e2e_${UUID.randomUUID()};DB_CLOSE_DELAY=-1;MODE=PostgreSQL"
        Database.connect(jdbcUrl, driver = "org.h2.Driver")
        transaction {
            SchemaUtils.create(OrdersTable, OrderItemsTable)
        }

        repository = ExposedOrderRepository()
        // Separate channels on one connection: publish path vs inbound lifecycle consumer (prod-like split).
        apiConnection = factory.newConnection()
        publishChannel = apiConnection.createChannel()
        consumerChannel = apiConnection.createChannel()
        RestaurantRabbitTopology.declareOrdersExchange(publishChannel, ordersExchange)
        RestaurantRabbitTopology.declareOrderingConsumerTopology(consumerChannel, ordersExchange)

        val publisher = RabbitOrderEventPublisher(publishChannel, ordersExchange, json)
        // Start inbound consumer before outbound simulators so order.preparing is not dropped.
        OrderingUpdatesConsumer(consumerChannel, repository, json).start()
        service = OrderApplicationService(repository, publisher)

        kitchen = KitchenSimulator(factory, json, ordersExchange)
        kitchen.start()
        runner = RunnerSimulator(factory, json, ordersExchange)
        runner.start()

        // Real Netty engine (not testApplication): long-lived server while Rabbit threads run.
        server = embeddedServer(Netty, host = "127.0.0.1", port = 0) {
            configureHttp(service)
        }
        server.start(wait = false)
        httpPort = runBlocking { server.engine.resolvedConnectors().first().port }
    }

    /**
     * Stop HTTP first, then simulators, then API Rabbit resources.
     *
     * [runCatching] ensures one close failure does not skip later cleanup. HTTP stops before Rabbit
     * because this test does not need graceful drain of in-flight requests.
     */
    @AfterEach
    fun tearDown() {
        runCatching { server.stop(1_000, 2_000) }
        runCatching { kitchen.close() }
        runCatching { runner.close() }
        runCatching { publishChannel.close() }
        runCatching { consumerChannel.close() }
        runCatching { apiConnection.close() }
    }

    /**
     * Happy-path lifecycle from guest order through kitchen, staff ready-to-serve, runner delivery,
     * and final `SERVED` visible on `GET /order/{id}`.
     *
     * **Preconditions:** Single order in flight; simulators ignore duplicate broker deliveries via
     * `CompletableDeferred.isCompleted` guards.
     *
     * **Postconditions:** Status [OrderStatus.SERVED]; line items unchanged from placement.
     *
     * **Not covered:** 404/409 on ready-to-serve ([PlaceOrderIntegrationTest]), DLQ poison handling,
     * duplicate redelivery regression.
     */
    @Test
    fun `order lifecycle from guest to served via rabbit kitchen and runner`() = runBlocking {
        val baseUrl = "http://127.0.0.1:$httpPort"
        HttpClient(CIO) {
            install(ContentNegotiation) {
                json(json)
            }
        }.use { http ->
            // --- Arrange: fixture uses two distinct menu lines (aggregated to 2 OrderLineEvent rows) ---
            val placeResponse = http.post("$baseUrl/order") {
                contentType(ContentType.Application.Json)
                setBody(
                    PlaceOrderRequest(
                        tableNumber = 12,
                        items = listOf(
                            PlaceOrderItemRequest(menuItemId = "caesar_salad", quantity = 1),
                            PlaceOrderItemRequest(menuItemId = "margherita_pizza", quantity = 2),
                        ),
                    ),
                )
            }
            // --- Act / assert: place persists PLACED and publishes order.new ---
            assertEquals(HttpStatusCode.Created, placeResponse.status)
            val orderId = placeResponse.body<PlaceOrderResponse>().orderId

            // --- Assert: kitchen received OrderPlacedEvent on kitchen.orders (not just HTTP 201) ---
            val seenByKitchen = withTimeout(45_000) { kitchen.orderSeen.await() }
            assertEquals(orderId, seenByKitchen.orderId)
            assertEquals(2, seenByKitchen.lines.size) // two menu lines, not three item rows

            // --- Assert: ordering consumer applied order.preparing → visible on read API ---
            withTimeout(45_000) {
                while (http.get("$baseUrl/order/$orderId").body<GetOrderResponse>().status != OrderStatus.PREPARING) {
                    delay(50) // tight poll: PREPARING usually follows quickly after kitchen publish
                }
            }

            // --- Act: staff marks ready (only HTTP mutation after place) → dish.ready ---
            assertEquals(
                HttpStatusCode.NoContent,
                http.post("$baseUrl/order/$orderId/ready-to-serve").status,
            )

            // --- Assert: runner consumed DishPreparedEvent with human-readable dish summary ---
            val seenByRunner = withTimeout(45_000) { runner.dishSeen.await() }
            assertEquals(orderId, seenByRunner.orderId)
            assertEquals(12, seenByRunner.tableNumber)
            assertTrue(seenByRunner.dishSummary.contains("Caesar Salad", ignoreCase = true))
            assertTrue(seenByRunner.dishSummary.contains("Margherita Pizza", ignoreCase = true))

            // --- Assert: delivery.confirmed propagated → SERVED on guest-facing GET ---
            val deadline = System.currentTimeMillis() + 30_000
            var last: GetOrderResponse? = null
            while (System.currentTimeMillis() < deadline) {
                last = http.get("$baseUrl/order/$orderId").body<GetOrderResponse>()
                if (last.status == OrderStatus.SERVED) break
                delay(150) // coarser poll: runner sleep(200) + consumer ack adds latency
            }
            assertEquals(
                OrderStatus.SERVED,
                last?.status,
                "Guest-facing status should reach SERVED (poll tolerates async / delayed broker delivery)",
            )

            // --- Assert: line items survived the full async lifecycle ---
            assertEquals(2, last!!.items.size)
            assertTrue(last.items.any { it.menuItemId == "caesar_salad" && it.quantity == 1 })
            assertTrue(last.items.any { it.menuItemId == "margherita_pizza" && it.quantity == 2 })
        }
    }

    /**
     * In-process stand-in for `kitchen-service`.
     *
     * **Parity with production:** [RestaurantRabbitTopology.QUEUE_KITCHEN], `basicQos(1)`,
     * [consumeManualAck], consumes `order.new`, publishes `order.preparing` with
     * [jsonPersistentMessageProperties].
     *
     * **Test-only simplifications:** no [com.restaurant.common.config.ConfigLoader] or connection retry;
     * [orderSeen] completes once; later messages are acked but ignored; shutdown via [CountDownLatch]
     * instead of a JVM shutdown hook.
     */
    private class KitchenSimulator(
        private val factory: ConnectionFactory,
        private val json: Json,
        private val ordersExchange: String,
    ) : AutoCloseable {
        /** Fulfilled when the first [OrderPlacedEvent] is consumed — used as a test synchronization point. */
        val orderSeen = CompletableDeferred<OrderPlacedEvent>()
        private var connection: Connection? = null
        private var worker: Thread? = null
        private val shutdown = CountDownLatch(1)

        /** Starts a dedicated consumer thread (separate connection, like the real kitchen process). */
        fun start() {
            worker = thread(name = "e2e-kitchen") {
                val conn = factory.newConnection()
                connection = conn
                val channel = conn.createChannel()
                RestaurantRabbitTopology.declareKitchenTopology(channel, ordersExchange)
                channel.basicQos(1)
                channel.consumeManualAck(RestaurantRabbitTopology.QUEUE_KITCHEN) { _, body ->
                    val placed = json.decodeFromString<OrderPlacedEvent>(
                        String(body, Charsets.UTF_8),
                    )
                    // Single-order test: ignore redeliveries after the first observed placement.
                    if (orderSeen.isCompleted) {
                        return@consumeManualAck
                    }
                    val preparing = OrderPreparingEvent(placed.orderId)
                    basicPublish(
                        ordersExchange,
                        RestaurantRabbitTopology.ROUTING_ORDER_PREPARING,
                        jsonPersistentMessageProperties(),
                        json.encodeToString(preparing).toByteArray(Charsets.UTF_8),
                    )
                    orderSeen.complete(placed)
                }
                try {
                    shutdown.await()
                } catch (_: InterruptedException) {
                    Thread.currentThread().interrupt()
                }
            }
        }

        /** Signals the worker to exit and closes the simulator's Rabbit connection. */
        override fun close() {
            shutdown.countDown()
            runCatching { connection?.close() }
            worker?.interrupt()
            worker?.join(10_000)
        }
    }

    /**
     * In-process stand-in for `runner-service`.
     *
     * **Parity with production:** [RestaurantRabbitTopology.QUEUE_RUNNER], `basicQos(1)`,
     * [consumeManualAck], consumes `dish.ready`, [Thread.sleep] (200 ms) then publishes
     * `delivery.confirmed` — same sequence as [com.restaurant.runner.RunnerService].
     *
     * **Test-only simplifications:** [dishSeen] completes once; duplicate deliveries ignored;
     * no delivery logging or ConfigLoader.
     */
    private class RunnerSimulator(
        private val factory: ConnectionFactory,
        private val json: Json,
        private val ordersExchange: String,
    ) : AutoCloseable {
        /** Fulfilled when the first [DishPreparedEvent] is consumed and delivery is published. */
        val dishSeen = CompletableDeferred<DishPreparedEvent>()
        private var connection: Connection? = null
        private var worker: Thread? = null
        private val shutdown = CountDownLatch(1)

        /** Starts a dedicated consumer thread (separate connection, like the real runner process). */
        fun start() {
            worker = thread(name = "e2e-runner") {
                val conn = factory.newConnection()
                connection = conn
                val channel = conn.createChannel()
                RestaurantRabbitTopology.declareRunnerTopology(channel, ordersExchange)
                channel.basicQos(1)
                channel.consumeManualAck(RestaurantRabbitTopology.QUEUE_RUNNER) { _, body ->
                    val dish = json.decodeFromString<DishPreparedEvent>(
                        String(body, Charsets.UTF_8),
                    )
                    if (dishSeen.isCompleted) {
                        return@consumeManualAck
                    }
                    // Mirrors runner-service: simulate walk-to-table before delivery.confirmed.
                    Thread.sleep(200)
                    val confirmed = DeliveryConfirmedEvent(dish.orderId)
                    basicPublish(
                        ordersExchange,
                        RestaurantRabbitTopology.ROUTING_DELIVERY_CONFIRMED,
                        jsonPersistentMessageProperties(),
                        json.encodeToString(confirmed).toByteArray(Charsets.UTF_8),
                    )
                    dishSeen.complete(dish)
                }
                try {
                    shutdown.await()
                } catch (_: InterruptedException) {
                    Thread.currentThread().interrupt()
                }
            }
        }

        /** Signals the worker to exit and closes the simulator's Rabbit connection. */
        override fun close() {
            shutdown.countDown()
            runCatching { connection?.close() }
            worker?.interrupt()
            worker?.join(10_000)
        }
    }
}
