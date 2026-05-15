package com.restaurant.ordering.infrastructure.web

import com.restaurant.ordering.application.OrderApplicationService
import com.restaurant.ordering.infrastructure.messaging.PublishNotAcknowledgedException
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.http.HttpStatusCode
import io.ktor.server.response.respond
import kotlinx.serialization.json.Json

/**
 * Installs Ktor plugins shared by all routes: JSON negotiation aligned with kotlinx.serialization
 * used for Rabbit payloads, and centralized mapping of validation failures to HTTP 400.
 */
fun Application.configureHttp(service: OrderApplicationService) {
    val json = Json {
        ignoreUnknownKeys = true
        encodeDefaults = true
    }
    install(ContentNegotiation) {
        json(json)
    }
    install(StatusPages) {
        exception<IllegalArgumentException> { call, cause ->
            call.respond(
                HttpStatusCode.BadRequest,
                ErrorResponse(cause.message ?: "bad request"),
            )
        }
        exception<PublishNotAcknowledgedException> { call, cause ->
            call.respond(
                HttpStatusCode.ServiceUnavailable,
                ErrorResponse(cause.message ?: "broker did not confirm publish"),
            )
        }
    }
    configureRouting(service)
}
