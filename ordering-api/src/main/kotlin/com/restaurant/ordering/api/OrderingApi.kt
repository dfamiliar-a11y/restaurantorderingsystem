package com.restaurant.ordering.api

import com.rabbitmq.client.ConnectionFactory
import com.restaurant.common.RabbitConnectionSupport
import com.restaurant.common.RestaurantRabbitTopology
import com.restaurant.common.config.ConfigLoader
import com.restaurant.ordering.application.OrderApplicationService
import com.restaurant.ordering.domain.OrderEventPublisher
import com.restaurant.ordering.infrastructure.messaging.OrderingUpdatesConsumer
import com.restaurant.ordering.infrastructure.messaging.RabbitOrderEventPublisher
import com.restaurant.ordering.infrastructure.persistence.ExposedOrderRepository
import com.restaurant.ordering.infrastructure.persistence.OrderItemsTable
import com.restaurant.ordering.infrastructure.persistence.OrdersTable
import com.restaurant.ordering.infrastructure.web.configureHttp
import io.ktor.server.config.HoconApplicationConfig
import io.ktor.server.engine.applicationEnvironment
import io.ktor.server.engine.connector
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory

/**
 * **Composition root** for the Ordering API: wires configuration, Exposed database schema,
 * RabbitMQ channels, and Ktor into a single deployable process.
 *
 * Messaging topology:
 * - **Outbound:** [RabbitOrderEventPublisher] publishes [OrderPlacedEvent] (`order.new`)
 *   and [DishPreparedEvent] (`dish.ready`) when staff POSTs ready-to-serve.
 * - **Inbound:** [OrderingUpdatesConsumer] applies Kitchen/Runner facts (`order.preparing`, `dish.ready`, `delivery.confirmed`)
 *   so HTTP reads stay consistent with the asynchronous pipeline.
 *
 * **Shutdown:** a JVM shutdown hook cancels the updates consumer, closes channels, then closes the connection with a
 * timeout so in-flight consumer work can finish before the process exits (best-effort under SIGTERM).
 */
fun main(@Suppress("UNUSED_PARAMETER") args: Array<String>) {
    val log = LoggerFactory.getLogger("OrderingApi")
    val rootConfig = ConfigLoader.resolvedRootConfig()
    val appConfig = ConfigLoader.load(rootConfig)

    val db = appConfig.database
    val dbUser = db.user
    if (!dbUser.isNullOrBlank()) {
        Database.connect(
            db.url,
            driver = db.driver,
            user = dbUser,
            password = db.password.orEmpty(),
        )
    } else {
        Database.connect(db.url, driver = db.driver)
    }

    transaction {
        SchemaUtils.create(OrdersTable, OrderItemsTable)
    }

    val orderRepository = ExposedOrderRepository()

    val mq = appConfig.rabbitMq
    val ordersExchange = mq.exchangeName
    val factory = ConnectionFactory().apply {
        host = mq.host
        port = mq.port
        username = mq.username
        password = mq.password
        virtualHost = mq.virtualHost
    }

    val connection = RabbitConnectionSupport.connectWithRetry(factory) { log.info(it) }
    connection.addShutdownListener { cause ->
        if (cause != null) {
            log.warn("RabbitMQ connection shut down: {}", cause.message)
        }
    }

    val publishChannel = connection.createChannel()
    RestaurantRabbitTopology.declareOrdersExchange(publishChannel, ordersExchange)

    val consumerChannel = connection.createChannel()
    RestaurantRabbitTopology.declareOrderingConsumerTopology(consumerChannel, ordersExchange)

    val publisher: OrderEventPublisher = RabbitOrderEventPublisher(publishChannel, ordersExchange)
    val updatesConsumer = OrderingUpdatesConsumer(consumerChannel, orderRepository)
    val updatesConsumerTag = updatesConsumer.start()

    Runtime.getRuntime().addShutdownHook(
        Thread {
            log.info("Shutdown hook: closing RabbitMQ resources")
            runCatching { consumerChannel.basicCancel(updatesConsumerTag) }
            runCatching { Thread.sleep(200) }
            runCatching { consumerChannel.close() }
            runCatching { publishChannel.close() }
            runCatching { connection.close(5_000) }
        },
    )

    val service = OrderApplicationService(orderRepository, publisher)

    val ktorRoot = rootConfig.getConfig("ktor")
    val deployment = ktorRoot.getConfig("deployment")
    val host = deployment.getString("host")
    val port = deployment.getInt("port")

    val environment = applicationEnvironment {
        this.log = LoggerFactory.getLogger("io.ktor.server.Application")
        config = HoconApplicationConfig(rootConfig)
    }

    embeddedServer(Netty, environment, configure = {
        connector {
            this.host = host
            this.port = port
        }
    }) {
        configureHttp(service)
    }.start(wait = true)
}
