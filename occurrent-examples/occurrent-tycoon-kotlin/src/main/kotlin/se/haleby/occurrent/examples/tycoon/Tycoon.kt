package se.haleby.occurrent.examples.tycoon

import kotlinx.collections.immutable.*
import se.haleby.occurrent.examples.tycoon.DomainEvent.*
import se.haleby.occurrent.examples.tycoon.VehicleActivity.EnRouteVehicleActivity
import se.haleby.occurrent.examples.tycoon.VehicleActivity.EnRouteVehicleActivity.DeliveringCargo
import se.haleby.occurrent.examples.tycoon.VehicleActivity.EnRouteVehicleActivity.Returning
import se.haleby.occurrent.examples.tycoon.VehicleActivity.WaitingForCargo

// Domain Model

typealias Hours = Int
typealias VehicleName = String

sealed class Location {
    override fun toString(): String = this::class.simpleName!!

    object Factory : Location()
    object Port : Location()
    object WarehouseA : Location()
    object WarehouseB : Location()
}

sealed class VehicleType {
    object Ship : VehicleType()
    object Truck : VehicleType()

    override fun toString(): String = this::class.simpleName!!
}

data class Vehicle(val name: VehicleName, val type: VehicleType)

data class Leg(val requiredVehicleType: VehicleType, val from: Location, val to: Location, val duration: Hours)

class DeliveryRoute internal constructor(internal val legs: MutableList<Leg>) {
    fun leg(requiredVehicleType: VehicleType, from: Location, to: Location, duration: Hours) {
        legs.add(Leg(requiredVehicleType, from, to, duration))
    }
}

class DeliveryNetwork private constructor(internal val routes: MutableList<DeliveryRoute>) {

    fun route(init: DeliveryRoute.() -> Unit) {
        val deliveryRoute = DeliveryRoute(mutableListOf())
        init(deliveryRoute)
        routes.add(deliveryRoute)
    }

    companion object {
        fun deliveryNetwork(init: DeliveryNetwork.() -> Unit): DeliveryNetwork {
            val deliveryNetwork = DeliveryNetwork(mutableListOf())
            init(deliveryNetwork)
            return deliveryNetwork
        }
    }
}

class Fleet private constructor(internal val vehicleLocations: MutableMap<Vehicle, Location>) {
    fun add(vehicleName: VehicleName, vehicleType: VehicleType, at: Location) {
        vehicleLocations[Vehicle(vehicleName, vehicleType)] = at
    }

    companion object {
        fun fleet(init: Fleet.() -> Unit): Fleet {
            val fleet = Fleet(mutableMapOf())
            init(fleet)
            return fleet
        }
    }
}

sealed class Cargo {
    object A : Cargo()
    object B : Cargo()

    override fun toString(): String = this::class.simpleName!!
}

class DeliveryPlan private constructor(internal val deliveries: MutableList<CargoDelivery>) {
    val size get() : Int = deliveries.size

    fun deliver(cargo: Cargo, from: Location, to: Location) {
        deliveries.add(CargoDelivery(cargo, from, to))
    }

    companion object {
        fun deliveryPlan(init: DeliveryPlan.() -> Unit): DeliveryPlan {
            val deliveryPlan = DeliveryPlan(mutableListOf())
            init(deliveryPlan)
            return deliveryPlan
        }
    }

    internal operator fun get(cargo: Cargo): CargoDelivery = deliveries.first { it.cargo == cargo }

    internal data class CargoDelivery(val cargo: Cargo, val from: Location, val to: Location)

    override fun toString(): String = "DeliveryPlan(deliveries=$deliveries, size=$size)"
}

sealed class DomainEvent {
    data class LegStarted(val vehicle: Vehicle, val from: Location, val to: Location, val estimatedTimeForThisLeg: Hours) : DomainEvent()
    data class LegCompleted(val vehicle: Vehicle, val from: Location, val to: Location, val elapsedTimeForThisLeg: Hours) : DomainEvent()
    data class VehicleStartedWaitingForCargo(val vehicle: Vehicle, val at: Location) : DomainEvent()
    data class VehicleStoppedWaitingForCargo(val vehicle: Vehicle, val at: Location) : DomainEvent()
    data class CargoWasDeliveredToDestination(val vehicle: Vehicle, val destination: Location, val elapsedTime: Hours) : DomainEvent()
    data class AllCargoHasBeenDelivered(val elapsedTime: Hours) : DomainEvent()
    data class TimeElapsed(val time: Hours) : DomainEvent()
}

// Use cases
fun deliverCargo(deliveryPlan: DeliveryPlan, fleet: Fleet, deliveryNetwork: DeliveryNetwork): List<DomainEvent> {
    val initialFleetActivity = fleet.vehicleLocations.mapValues { (_, location) ->
        WaitingForCargo(location = location)
    }.toPersistentMap()

    val cargoAtLocation = deliveryPlan.deliveries.fold(mutableMapOf<Location, PersistentList<Cargo>>()) { m, delivery ->
        m.compute(delivery.from) { _, cargoList ->
            cargoList?.add(delivery.cargo) ?: persistentListOf(delivery.cargo)
        }
        m
    }.let { CargoAtLocation(it.toPersistentMap()) }

    val journeyAtStartOfDelivery = Journey(
            fleetActivity = initialFleetActivity,
            cargoAtLocation = cargoAtLocation,
            deliveryPlan = deliveryPlan,
            deliveryNetwork = deliveryNetwork
    )

    return generateSequence(journeyAtStartOfDelivery, Journey::proceed)
            .dropWhile { journey -> !journey.isCompleted() }
            .first()
            .history
}

// Internal
private sealed class VehicleActivity {

    sealed class EnRouteVehicleActivity : VehicleActivity() {
        abstract val from: Location
        abstract val to: Location
        abstract val elapsedTime: Hours
        abstract val legTime: Hours

        val remainingTime: Hours get() = legTime - elapsedTime
        fun hasArrived(): Boolean = remainingTime == 0
        fun hasArrivedTo(location: Location) = to == location

        data class DeliveringCargo(val cargo: Cargo, override val from: Location, override val to: Location, override val legTime: Hours, override val elapsedTime: Hours = 0) : EnRouteVehicleActivity() {
            fun continueRoute(): DeliveringCargo = copy(elapsedTime = elapsedTime.inc())
        }

        data class Returning(override val from: Location, override val to: Location, override val legTime: Hours, override val elapsedTime: Hours = 0) : EnRouteVehicleActivity() {
            fun continueRoute(): Returning = copy(elapsedTime = elapsedTime.inc())
        }
    }

    data class WaitingForCargo(val location: Location) : VehicleActivity()
}

private data class Journey(val fleetActivity: PersistentMap<Vehicle, VehicleActivity>,
                           val elapsedTime: Hours = 0,
                           val deliveryPlan: DeliveryPlan,
                           val cargoAtLocation: CargoAtLocation,
                           val history: PersistentList<DomainEvent> = persistentListOf(),
                           val deliveryNetwork: DeliveryNetwork) {

    fun proceed(): Journey {
        val journeyAfterAllVehiclesHaveMoved = fleetActivity.entries.fold(this) { currentJourney, (vehicle, currentActivity) ->
            when (currentActivity) {
                is WaitingForCargo -> currentJourney.loadOrWaitForCargo(vehicle, currentActivity.location, currentActivity)
                is EnRouteVehicleActivity -> currentJourney.continueRoute(vehicle, currentActivity)
            }
        }
        return if (journeyAfterAllVehiclesHaveMoved.isCompleted()) {
            journeyAfterAllVehiclesHaveMoved
        } else {
            journeyAfterAllVehiclesHaveMoved.elapseTimeBy(1)
        }
    }

    fun isCompleted(): Boolean = history.lastOrNull() is AllCargoHasBeenDelivered

    // private functions

    private fun loadOrWaitForCargo(vehicle: Vehicle, location: Location, currentVehicleActivity: VehicleActivity): Journey {
        val cargo = cargoAtLocation.findCargoAtLocation(location)
        return when {
            cargo == null && currentVehicleActivity is WaitingForCargo -> this // Vehicle is already waiting for cargo, do nothing
            cargo == null ->
                // Vehicle is currently not waiting for cargo (it has just arrived to the location) and since
                // there's no cargo at this location the vehicle needs to wait for it to arrive before it can proceed.
                copy(fleetActivity = fleetActivity.put(vehicle, WaitingForCargo(location)),
                        history = history.add(VehicleStartedWaitingForCargo(vehicle, location)))
            else -> {
                val (requiredVehicleType, from, to, duration) = deliveryNetwork.findRouteForCargo(cargo, deliveryPlan).findLeg { leg -> leg.from == location }!!
                if (requiredVehicleType != vehicle.type) {
                    return this
                }
                val isCurrentlyWaitingForCargo = fleetActivity[vehicle] is WaitingForCargo
                val legStarted = LegStarted(vehicle, from, to, duration)
                val events = if (isCurrentlyWaitingForCargo) listOf(VehicleStoppedWaitingForCargo(vehicle, from), legStarted) else listOf(legStarted)
                copy(fleetActivity = fleetActivity.put(vehicle, DeliveringCargo(cargo, from, to, duration)),
                        history = history.addAll(events),
                        cargoAtLocation = cargoAtLocation.addCargoToLocation(from))
            }
        }
    }

    private fun continueRoute(vehicle: Vehicle, vehicleActivity: EnRouteVehicleActivity): Journey = when (vehicleActivity) {
        is DeliveringCargo -> {
            val vehicleActivityAfterRouteWasContinued = vehicleActivity.continueRoute()
            if (vehicleActivityAfterRouteWasContinued.hasArrived()) {
                val currentLocation = vehicleActivity.to
                val deliveringCargoEvents = mutableListOf<DomainEvent>()
                val finalCargoDestination = deliveryPlan[vehicleActivityAfterRouteWasContinued.cargo].to
                if (vehicleActivityAfterRouteWasContinued.hasArrivedTo(finalCargoDestination)) {
                    deliveringCargoEvents.add(CargoWasDeliveredToDestination(vehicle, vehicleActivityAfterRouteWasContinued.to, elapsedTime))
                }
                deliveringCargoEvents.add(LegCompleted(vehicle, vehicleActivity.from, vehicleActivity.to, vehicleActivityAfterRouteWasContinued.elapsedTime))
                val newVehicleActivity = Returning(from = vehicleActivity.to, to = vehicleActivity.from, legTime = vehicleActivity.legTime)
                appendHistory(deliveringCargoEvents).unloadCargo(vehicleActivityAfterRouteWasContinued.cargo, currentLocation).updateVehicleActivity(vehicle, newVehicleActivity)
            } else {
                updateVehicleActivity(vehicle, vehicleActivityAfterRouteWasContinued)
            }
        }
        is Returning -> {
            val vehicleActivityAfterMove = vehicleActivity.continueRoute()
            if (vehicleActivityAfterMove.hasArrived()) {
                val currentLocation = vehicleActivity.to
                loadOrWaitForCargo(vehicle, currentLocation, vehicleActivityAfterMove)
            } else {
                updateVehicleActivity(vehicle, vehicleActivityAfterMove)
            }
        }
    }

    private fun elapseTimeBy(time: Hours) = copy(elapsedTime = elapsedTime + time, history = history.add(TimeElapsed(time)))
    private fun appendHistory(event: DomainEvent): Journey = copy(history = history.add(event))
    private fun appendHistory(events: List<DomainEvent>): Journey = copy(history = history.addAll(events))

    private fun updateVehicleActivity(vehicle: Vehicle, vehicleActivity: VehicleActivity): Journey {
        return copy(fleetActivity = fleetActivity.put(vehicle, vehicleActivity))
    }

    private fun unloadCargo(cargo: Cargo, location: Location): Journey {
        val s = if (history.count { it is CargoWasDeliveredToDestination } == deliveryPlan.size) {
            appendHistory(AllCargoHasBeenDelivered(elapsedTime))
        } else {
            this
        }
        return s.copy(cargoAtLocation = cargoAtLocation.removeCargoFromLocation(cargo, location))
    }
}

private fun DeliveryNetwork.findRouteForCargo(cargo: Cargo, deliveryPlan: DeliveryPlan): DeliveryRoute = routes.first { route -> route.legs.any { leg -> leg.to == deliveryPlan[cargo].to } }

private fun DeliveryRoute.findLeg(predicate: (Leg) -> Boolean) = legs.firstOrNull(predicate)

private data class CargoAtLocation(private val buffer: PersistentMap<Location, PersistentList<Cargo>> = persistentMapOf()) {
    fun removeCargoFromLocation(cargo: Cargo, location: Location): CargoAtLocation = copy(buffer = buffer.put(location, buffer.getOrDefault(location, persistentListOf()).add(cargo)))
    fun findCargoAtLocation(location: Location): Cargo? = buffer[location]?.firstOrNull()
    fun addCargoToLocation(location: Location): CargoAtLocation = copy(buffer = buffer.put(location, buffer[location]!!.removeAt(0)))
}