package com.restaurant.common

import kotlinx.serialization.Serializable

/**
 * Cross-service lifecycle for a single order aggregate, carried on HTTP responses and advanced
 * asynchronously when Kitchen and Runner publish their respective routing keys.
 *
 * **Shared contract:** the same `@Serializable` enum is embedded in REST bodies and Rabbit JSON
 * payloads. Treat value renames or reordering as a **breaking protocol change**; prefer additive
 * values and explicit versioning when evolving beyond this demo.
 */
@Serializable
enum class OrderStatus {
    PLACED,
    PREPARING,
    READY_TO_SERVE,
    SERVED,
}
