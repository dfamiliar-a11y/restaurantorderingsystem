package com.restaurant.common

import kotlinx.serialization.Serializable

@Serializable
data class MenuItem(
    val id: String,
    val name: String,
    val price: Double,
)

/**
 * Static catalog for the demo system: shared reference data consulted by Ordering when validating
 * [com.restaurant.ordering.application.OrderApplicationService.placeOrder] and when emitting
 * priced line snapshots on [com.restaurant.common.events.OrderPlacedEvent].
 */
object Menu {
    val items: List<MenuItem> = listOf(
        MenuItem("caesar_salad", "Caesar Salad", 12.0),
        MenuItem("margherita_pizza", "Margherita Pizza", 15.0),
        MenuItem("pasta_carbonara", "Pasta Carbonara", 14.0),
        MenuItem("beef_burger", "Beef Burger", 16.0),
        MenuItem("chocolate_fondant", "Chocolate Fondant", 8.0),
        MenuItem("truffle_fries", "Truffle Fries", 7.0),
        MenuItem("craft_beer", "Craft Beer", 6.0),
    )

    private val byId: Map<String, MenuItem> = items.associateBy { it.id }

    fun findById(id: String): MenuItem? = byId[id]
}
