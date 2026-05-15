package com.restaurant.common

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Channel
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import kotlinx.serialization.SerializationException
import org.slf4j.Logger

/**
 * Returns `false` when the failure is almost certainly a **poison message** (bad payload) so the
 * caller should **not** requeue: with a dead-letter exchange on the queue, `basicNack(..., requeue=false)`
 * routes the message to a DLQ instead of spinning forever.
 *
 * Transient failures (for example database errors) return `true` so `basicNack(..., requeue=true)`
 * can retry. That can still spin on a permanently broken row—called out in service READMEs.
 */
fun shouldRequeueManualAckFailure(e: Exception): Boolean {
    var t: Throwable? = e
    while (t != null) {
        if (t is SerializationException) return false
        t = t.cause
    }
    return true
}

/**
 * Registers a consumer with manual acknowledgement: after [onDelivery] completes without throwing,
 * the message is acknowledged; on failure it is negatively acknowledged. Requeue is chosen via
 * [shouldRequeueManualAckFailure] unless overridden by [requeueOnFailure].
 *
 * @param requeueOnFailure when non-null, called with the exception to decide requeue (overrides
 *   the default serialization vs transient heuristic).
 * @param onFailure optional hook for logging or metrics; when null and [log] is non-null,
 * [errorMessage] is logged with SLF4J.
 */
fun Channel.consumeManualAck(
    queue: String,
    autoAck: Boolean = false,
    log: Logger? = null,
    errorMessage: String = "Failed to process message",
    requeueOnFailure: ((Exception) -> Boolean)? = null,
    onFailure: ((Envelope, Exception) -> Unit)? = null,
    onDelivery: Channel.(envelope: Envelope, body: ByteArray) -> Unit,
): String {
    val ch = this
    return ch.basicConsume(
        queue,
        autoAck,
        object : DefaultConsumer(ch) {
            override fun handleDelivery(
                consumerTag: String?,
                envelope: Envelope,
                properties: AMQP.BasicProperties?,
                body: ByteArray,
            ) {
                val deliveryTag = envelope.deliveryTag
                try {
                    ch.onDelivery(envelope, body)
                    ch.basicAck(deliveryTag, false)
                } catch (e: Exception) {
                    onFailure?.invoke(envelope, e) ?: log?.error(errorMessage, e)
                    val requeue = requeueOnFailure?.invoke(e) ?: shouldRequeueManualAckFailure(e)
                    ch.basicNack(deliveryTag, false, requeue)
                }
            }
        },
    )
}
