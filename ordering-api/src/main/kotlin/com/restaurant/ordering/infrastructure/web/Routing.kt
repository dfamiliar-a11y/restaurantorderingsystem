package com.restaurant.ordering.infrastructure.web

import com.restaurant.ordering.application.MarkReadyToServeResult
import com.restaurant.ordering.application.OrderApplicationService
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import io.ktor.server.routing.routing

/**
 * HTTP **adapter** mapping Ktor routes to [OrderApplicationService]: translates JSON DTOs to
 * application primitives and maps domain outcomes to status codes (201 / 204 / 400 / 404 / 409).
 */
fun Application.configureRouting(service: OrderApplicationService) {
    routing {
        route("/menu") {
            get {
                call.respond(service.getMenu())
            }
        }
        route("/order") {
            post {
                val body = call.receive<PlaceOrderRequest>()
                val pairs = body.items.map { it.menuItemId to it.quantity }
                val orderId = service.placeOrder(body.tableNumber, pairs)
                call.respond(HttpStatusCode.Created, PlaceOrderResponse(orderId))
            }
            get("/{id}") {
                val id = call.parameters["id"] ?: return@get call.respond(
                    HttpStatusCode.BadRequest,
                    ErrorResponse("missing id"),
                )
                val order = service.getOrder(id) ?: return@get call.respond(
                    HttpStatusCode.NotFound,
                    ErrorResponse("order not found"),
                )
                call.respond(
                    GetOrderResponse(
                        orderId = order.id,
                        tableNumber = order.tableNumber,
                        status = order.status,
                        items = order.items.map {
                            OrderItemResponse(
                                menuItemId = it.menuItemId,
                                name = it.name,
                                quantity = it.quantity,
                                unitPrice = it.unitPrice,
                            )
                        },
                    ),
                )
            }
            post("/{id}/ready-to-serve") {
                val id = call.parameters["id"] ?: return@post call.respond(
                    HttpStatusCode.BadRequest,
                    ErrorResponse("missing id"),
                )
                when (val outcome = service.markReadyToServe(id)) {
                    MarkReadyToServeResult.Published,
                    MarkReadyToServeResult.AlreadyReady,
                    -> call.respond(HttpStatusCode.NoContent)
                    MarkReadyToServeResult.OrderNotFound -> call.respond(
                        HttpStatusCode.NotFound,
                        ErrorResponse("order not found"),
                    )
                    is MarkReadyToServeResult.InvalidStatus -> call.respond(
                        HttpStatusCode.Conflict,
                        ErrorResponse(
                            "order cannot be marked ready from status ${outcome.status}",
                        ),
                    )
                }
            }
        }
    }
}
