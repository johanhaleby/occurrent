package se.haleby.occurrent.examples.tycoon

import kotlinx.collections.immutable.*
import se.haleby.occurrent.examples.tycoon.DomainEvent.*
import se.haleby.occurrent.examples.tycoon.VehicleActivity.*
import se.haleby.occurrent.examples.tycoon.VehicleActivity.EnRouteVehicleActivity.DeliveringCargo
import se.haleby.occurrent.examples.tycoon.VehicleActivity.EnRouteVehicleActivity.Returning
import se.haleby.occurrent.examples.tycoon.VehicleActivity.StationaryVehicleActivity.WaitingForCargo
import se.haleby.occurrent.examples.tycoon.VehicleActivity.StationaryVehicleActivity.WaitingToStartJourney

// Domain Model

typealias Hours = Int
typealias VehicleName = String

enum class Location {
    Factory, Port, WarehouseA, WarehouseB
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

class DeliveryPlan private constructor(internal val cargoDeliveries: MutableList<CargoDelivery>) {
    val size get() = cargoDeliveries.size

    fun deliver(cargo: Cargo, from: Location, to: Location) {
        cargoDeliveries.add(CargoDelivery(cargo, from, to))
    }

    companion object {
        fun deliveryPlan(init: DeliveryPlan.() -> Unit): DeliveryPlan {
            val deliveryPlan = DeliveryPlan(mutableListOf())
            init(deliveryPlan)
            return deliveryPlan
        }
    }

    internal operator fun get(cargo: Cargo): CargoDelivery = cargoDeliveries.first { it.cargo == cargo }

    internal data class CargoDelivery(val cargo: Cargo, val from: Location, val to: Location)

    override fun toString(): String = "DeliveryPlan(deliveries=$cargoDeliveries)"
}

sealed class DomainEvent {
    data class LegStarted(val cargo: Cargo, val vehicle: Vehicle, val from: Location, val to: Location, val estimatedTimeForThisLeg: Hours) : DomainEvent()
    data class LegCompleted(val cargo: Cargo, val vehicle: Vehicle, val from: Location, val to: Location, val elapsedTimeForThisLeg: Hours) : DomainEvent()
    data class VehicleStartedWaitingForCargo(val vehicle: Vehicle, val at: Location) : DomainEvent()
    data class VehicleStoppedWaitingForCargo(val vehicle: Vehicle, val at: Location) : DomainEvent()
    data class CargoWasDeliveredToDestination(val cargo: Cargo, val vehicle: Vehicle, val destination: Location, val elapsedTime: Hours) : DomainEvent()
    data class AllCargoHasBeenDelivered(val elapsedTime: Hours) : DomainEvent()
    data class TimeElapsed(val time: Hours) : DomainEvent()
}

// Use cases
fun deliverCargo(deliveryPlan: DeliveryPlan, fleet: Fleet, deliveryNetwork: DeliveryNetwork): List<DomainEvent> {
    val initialFleetActivity = fleet.vehicleLocations.mapValues { (_, location) ->
        WaitingToStartJourney(location = location)
    }.toPersistentMap()

    val facilitiesWithoutStock = Location.values().fold(Facilities()) { facilities, location ->
        facilities.addFacility(location, Facility())
    }

    val facilitiesWithStock = deliveryPlan.cargoDeliveries.fold(facilitiesWithoutStock) { facilities, (cargo, location) ->
        facilities.unloadCargo(location, cargo)
    }

    val journeyAtStartOfDelivery = Journey(
            fleetActivity = initialFleetActivity,
            facilities = facilitiesWithStock,
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

    sealed class StationaryVehicleActivity : VehicleActivity() {
        abstract val location: Location
        data class WaitingForCargo(override val location: Location) : StationaryVehicleActivity()
        data class WaitingToStartJourney(override val location: Location) : StationaryVehicleActivity()

    }
}

private data class Journey(val fleetActivity: PersistentMap<Vehicle, VehicleActivity>,
                           val elapsedTime: Hours = 0,
                           val deliveryPlan: DeliveryPlan,
                           val facilities: Facilities,
                           val history: PersistentList<DomainEvent> = persistentListOf(),
                           val deliveryNetwork: DeliveryNetwork) {

    fun proceed(): Journey {
        val journeyAfterAllVehiclesHaveMoved = fleetActivity.entries.fold(this) { currentJourney, (vehicle, currentActivity) ->
            when (currentActivity) {
                is StationaryVehicleActivity -> currentJourney.loadOrWaitForCargo(vehicle, currentActivity.location, currentActivity)
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
        val cargo = facilities[location].firstStockedCargo()
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
                val legStarted = LegStarted(cargo, vehicle, from, to, duration)
                val events = if (isCurrentlyWaitingForCargo) listOf(VehicleStoppedWaitingForCargo(vehicle, from), legStarted) else listOf(legStarted)
                copy(fleetActivity = fleetActivity.put(vehicle, DeliveringCargo(cargo, from, to, duration)),
                        history = history.addAll(events),
                        facilities = facilities.loadCargo(from, cargo))
            }
        }
    }

    private fun continueRoute(vehicle: Vehicle, vehicleActivity: EnRouteVehicleActivity): Journey = when (vehicleActivity) {
        is DeliveringCargo -> {
            val vehicleActivityAfterRouteWasContinued = vehicleActivity.continueRoute()
            if (vehicleActivityAfterRouteWasContinued.hasArrived()) {
                val currentLocation = vehicleActivity.to
                val cargo = vehicleActivity.cargo
                val deliveringCargoEvents = mutableListOf<DomainEvent>(LegCompleted(cargo, vehicle, vehicleActivity.from, vehicleActivity.to, vehicleActivityAfterRouteWasContinued.elapsedTime))
                val finalCargoDestination = deliveryPlan[cargo].to
                if (vehicleActivityAfterRouteWasContinued.hasArrivedTo(finalCargoDestination)) {
                    deliveringCargoEvents.add(CargoWasDeliveredToDestination(cargo, vehicle, vehicleActivityAfterRouteWasContinued.to, elapsedTime))
                    if (history.count { it is CargoWasDeliveredToDestination }.inc() == deliveryPlan.size) {
                        deliveringCargoEvents.add(AllCargoHasBeenDelivered(elapsedTime))
                    }
                }
                val newVehicleActivity = Returning(from = vehicleActivity.to, to = vehicleActivity.from, legTime = vehicleActivity.legTime)
                copy(fleetActivity = fleetActivity.put(vehicle, newVehicleActivity),
                        history = history.addAll(deliveringCargoEvents),
                        facilities = facilities.unloadCargo(currentLocation, cargo)
                )
            } else {
                copy(fleetActivity = fleetActivity.put(vehicle, vehicleActivityAfterRouteWasContinued))
            }
        }
        is Returning -> {
            val vehicleActivityAfterMove = vehicleActivity.continueRoute()
            if (vehicleActivityAfterMove.hasArrived()) {
                val currentLocation = vehicleActivity.to
                loadOrWaitForCargo(vehicle, currentLocation, vehicleActivityAfterMove)
            } else {
                copy(fleetActivity = fleetActivity.put(vehicle, vehicleActivityAfterMove))
            }
        }
    }

    private fun elapseTimeBy(time: Hours) = copy(elapsedTime = elapsedTime + time, history = history.add(TimeElapsed(time)))
}

private fun DeliveryNetwork.findRouteForCargo(cargo: Cargo, deliveryPlan: DeliveryPlan): DeliveryRoute = routes.first { route -> route.legs.any { leg -> leg.to == deliveryPlan[cargo].to } }

private fun DeliveryRoute.findLeg(predicate: (Leg) -> Boolean) = legs.firstOrNull(predicate)

private data class Facilities(private val facilityAt: PersistentMap<Location, Facility> = persistentMapOf()) {
    operator fun get(location: Location): Facility = facilityAt[location]!!
    fun addFacility(location: Location, facility: Facility): Facilities = copy(facilityAt = facilityAt.put(location, facility))
    fun unloadCargo(location: Location, cargo: Cargo): Facilities = withCargo(location) { it.unloadCargo(cargo) }
    fun loadCargo(location: Location, cargo: Cargo): Facilities = withCargo(location) { it.loadCargo(cargo) }

    private fun withCargo(location: Location, fn: (Facility) -> Facility): Facilities {
        val updatedFacility = fn(facilityAt[location]!!)
        return copy(facilityAt = facilityAt.put(location, updatedFacility))
    }
}

private data class Facility(val stock: PersistentList<Cargo> = persistentListOf()) {
    fun loadCargo(cargo: Cargo): Facility = copy(stock = stock.remove(cargo))
    fun unloadCargo(cargo: Cargo): Facility = copy(stock = stock.add(cargo))
    fun firstStockedCargo(): Cargo? = stock.firstOrNull()
}