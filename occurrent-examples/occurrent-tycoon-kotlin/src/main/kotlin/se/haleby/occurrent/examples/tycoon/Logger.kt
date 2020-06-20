package se.haleby.occurrent.examples.tycoon

import kotlinx.collections.immutable.PersistentList
import kotlinx.collections.immutable.PersistentMap
import kotlinx.collections.immutable.persistentListOf
import kotlinx.collections.immutable.persistentMapOf
import se.haleby.occurrent.examples.tycoon.Cargo.A
import se.haleby.occurrent.examples.tycoon.Cargo.B
import se.haleby.occurrent.examples.tycoon.DomainEvent.*
import se.haleby.occurrent.examples.tycoon.VehicleType.Ship
import se.haleby.occurrent.examples.tycoon.VehicleType.Truck

private typealias OriginAndDestination = Pair<Location, Location>
private typealias OriginAndDestinationForCargo = PersistentMap<Cargo, Pair<Location, Location>>

private fun event(event: String, time: Int, transportId: Int, kind: String, location: String, destination: String, cargoId: Int, cargoDestination: String, cargoOrigin: String): String =
        """{"event": "$event", "time": $time, "transport_id": $transportId , "kind": "$kind", "location": "$location", "destination": "$destination", "cargo": [{"cargo_id": $cargoId, "destination": "$cargoDestination", "origin": "$cargoOrigin"}]}"""

private val Vehicle.id
    get() = when (name) {
        "A" -> 0
        "B" -> 1
        "Ship" -> 2
        else -> throw IllegalStateException("Invalid vehicle id")
    }

private val Vehicle.kind
    get() = when (type) {
        Ship -> "SHIP"
        Truck -> "TRUCK"
    }

private val Cargo.id
    get() = when (this) {
        A -> 0
        B -> 1
    }

private fun Location.serialize() = toString().toUpperCase()

// public
fun generateLogFromEvents(domainEvents: List<DomainEvent>): List<String> {
    data class State(val originAndDestinationForCargo: OriginAndDestinationForCargo, val events: PersistentList<String>) {
        constructor() : this(persistentMapOf<Cargo, OriginAndDestination>(), persistentListOf<String>())
    }

    val (_, logEvents) = domainEvents.fold(State()) { state, domainEvent ->
        when (domainEvent) {
            is CargoDeliveryStarted -> state.copy(originAndDestinationForCargo = state.originAndDestinationForCargo.put(domainEvent.cargo, OriginAndDestination(domainEvent.origin, domainEvent.destination)))
            is VehicleDeparted -> {
                val (origin, destination) = state.originAndDestinationForCargo[domainEvent.cargo]!!
                state.copy(events = state.events.add(event("DEPART", domainEvent.elapsedTime, domainEvent.vehicle.id, domainEvent.vehicle.kind,
                        domainEvent.from.serialize(), domainEvent.to.serialize(),
                        domainEvent.cargo.id, origin.serialize(), destination.serialize())))
            }
            is VehicleArrived -> {
                val (origin, destination) = state.originAndDestinationForCargo[domainEvent.cargo]!!
                state.copy(events = state.events.add(event("ARRIVED", domainEvent.elapsedTime, domainEvent.vehicle.id, domainEvent.vehicle.kind,
                        domainEvent.from.serialize(), domainEvent.to.serialize(),
                        domainEvent.cargo.id, origin.serialize(), destination.serialize())))
            }
            else -> state
        }
    }

    return logEvents
}