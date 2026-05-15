package com.restaurant.ordering.infrastructure.messaging

/**
 * Raised when the broker did not **publisher-ack** a message within the configured wait window.
 *
 * The order row may already be committed (**dual-write** gap); callers typically map this to HTTP 503
 * so a client can retry. A production system would add an **outbox** or idempotent `POST /order` to
 * make retries safe—documented on [com.restaurant.ordering.application.OrderApplicationService].
 */
class PublishNotAcknowledgedException(message: String, cause: Throwable? = null) :
    Exception(message, cause)
