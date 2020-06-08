package se.haleby.occurrent.examples.tycoon

import kotlinx.collections.immutable.*
import se.haleby.occurrent.examples.tycoon.Destination.WarehouseA
import se.haleby.occurrent.examples.tycoon.Destination.WarehouseB
import se.haleby.occurrent.examples.tycoon.DomainEvent.*
import se.haleby.occurrent.examples.tycoon.StartingPoint.Factory
import se.haleby.occurrent.examples.tycoon.TransitPoint.Port
import se.haleby.occurrent.examples.tycoon.VehicleActivity.MovableVehicleActivity.DeliveringCargo
import se.haleby.occurrent.examples.tycoon.VehicleActivity.MovableVehicleActivity.Returning
import se.haleby.occurrent.examples.tycoon.VehicleActivity.WaitingForCargo
import se.haleby.occurrent.examples.tycoon.VehicleType.Ship
import se.haleby.occurrent.examples.tycoon.VehicleType.Truck

// Domain Model

typealias Hours = Int
typealias VehicleName = String

sealed class Location {
    override fun toString(): String = this::class.simpleName!!
}

sealed class Destination : Location() {
    object WarehouseA : Destination()
    object WarehouseB : Destination()
}

sealed class StartingPoint : Location() {
    object Factory : StartingPoint()
}

sealed class TransitPoint : Location() {
    object Port : TransitPoint()
}

sealed class VehicleType {
    object Ship : VehicleType()
    object Truck : VehicleType()

    override fun toString(): String = this::class.simpleName!!
}

data class Vehicle(val name: VehicleName, val type: VehicleType)

data class Leg(val requiredVehicleType: VehicleType, val from: Location, val to: Location, val duration: Hours)

data class Route(val legs: List<Leg>) : Iterable<Leg> {
    constructor(vararg legs: Leg) : this(legs.toList())

    override fun iterator(): Iterator<Leg> = legs.listIterator()

    fun findLeg(predicate: (Leg) -> Boolean) = legs.firstOrNull(predicate)
}

data class DeliveryNetwork(val network: List<Route>) {
    constructor(vararg routes: Route) : this(routes.toList())
}

data class Fleet(val vehicles: List<Vehicle>) : Iterable<Vehicle> {
    constructor(vararg vehicles: Vehicle) : this(vehicles.toList())

    override fun iterator(): Iterator<Vehicle> = vehicles.listIterator()
}

sealed class Cargo {
    object A : Cargo()
    object B : Cargo()

    override fun toString(): String = this::class.simpleName!!
}

sealed class DomainEvent {
    data class LegStarted(val vehicle: Vehicle, val from: Location, val to: Location, val estimatedTimeForThisLeg: Hours) : DomainEvent()
    data class LegCompleted(val vehicle: Vehicle, val from: Location, val to: Location, val elapsedTimeForThisLeg: Hours) : DomainEvent()
    data class VehicleStartedWaitingForCargo(val vehicle: Vehicle, val at: Location) : DomainEvent()
    data class VehicleStoppedWaitingForCargo(val vehicle: Vehicle, val at: Location) : DomainEvent()
    data class CargoWasDeliveredToDestination(val vehicle: Vehicle, val destination: Destination, val elapsedTime: Hours) : DomainEvent()
    data class AllCargoWasDelivered(val elapsedTime: Hours) : DomainEvent()
    data class TimePassed(val time: Hours) : DomainEvent()
}

// Legs

val factoryToPort = Leg(requiredVehicleType = Truck, from = Factory, to = Port, duration = 1)
val portToWarehouseA = Leg(requiredVehicleType = Ship, from = Port, to = WarehouseA, duration = 4)
val factoryToWarehouseB = Leg(requiredVehicleType = Truck, from = Factory, to = WarehouseB, duration = 5)

// Routes
val routeFromFactoryToWarehouseA = Route(factoryToPort, portToWarehouseA)
val routeFromFactoryToWarehouseB = Route(factoryToWarehouseB)

// Delivery network
val deliveryNetwork = DeliveryNetwork(routeFromFactoryToWarehouseA, routeFromFactoryToWarehouseB)

// Fleet
val fleet = Fleet(Vehicle(name = "A", type = Truck), Vehicle(name = "B", type = Truck), Vehicle(name = "Ship", type = Ship))


// Use cases


private sealed class VehicleActivity {
    abstract val vehicle: Vehicle

    sealed class MovableVehicleActivity<T : MovableVehicleActivity<T>> : VehicleActivity() {
        abstract val from: Location
        abstract val to: Location
        abstract val elapsedTime: Hours
        abstract val legTime: Hours

        abstract fun move(): T

        val remainingTime: Hours get() = legTime - elapsedTime
        fun hasArrived(): Boolean = remainingTime == 0

        data class DeliveringCargo(val cargo: Cargo, override val vehicle: Vehicle, override val from: Location, override val to: Location, override val legTime: Hours, override val elapsedTime: Hours = 0) : MovableVehicleActivity<DeliveringCargo>() {
            override fun move(): DeliveringCargo = copy(elapsedTime = elapsedTime.inc())
        }

        data class Returning(override val vehicle: Vehicle, override val from: Location, override val to: Location, override val legTime: Hours, override val elapsedTime: Hours = 0) : MovableVehicleActivity<Returning>() {
            override fun move(): Returning = copy(elapsedTime = elapsedTime.inc())
        }

    }

    data class WaitingForCargo(override val vehicle: Vehicle, val location: Location) : VehicleActivity()
}

private data class DeliveryOverview(val vehicleActivities: PersistentList<VehicleActivity>,
                                    val elapsedTime: Hours = 0,
                                    val cargoDestinations: ImmutableMap<Cargo, Destination>,
                                    val locationBuffers: LocationBuffers = LocationBuffers(),
                                    val history: PersistentList<DomainEvent> = persistentListOf(),
                                    val completed: Boolean = false) : Iterable<VehicleActivity> {
    fun passTime(time: Hours) = copy(elapsedTime = elapsedTime + time, history = history.add(TimePassed(time)))
    fun appendHistory(e: DomainEvent): DeliveryOverview = copy(history = history.add(e))
    fun appendHistory(events: List<DomainEvent>): DeliveryOverview = copy(history = history.addAll(events))

    override fun iterator(): Iterator<VehicleActivity> = vehicleActivities.listIterator()

    fun updateVehicleActivity(vehicleActivity: VehicleActivity): DeliveryOverview {
        val updatedVehicleActivities = replaceVehicleActivityForVehicle(vehicleActivity)
        return copy(vehicleActivities = updatedVehicleActivities)
    }

    fun dropOffCargo(cargo: Cargo, location: Location): DeliveryOverview = copy(locationBuffers = locationBuffers.dropOffCargo(cargo, location))

    fun pickUpOrWaitForCargo(vehicle: Vehicle, location: Location, deliveryNetwork: DeliveryNetwork): DeliveryOverview {
        val cargo = locationBuffers.findStockedCargoAt(location)
        return if (cargo == null) {
            copy(vehicleActivities = replaceVehicleActivityForVehicle(WaitingForCargo(vehicle, location)),
                    history = history.add(VehicleStartedWaitingForCargo(vehicle, location)))
        } else {
            val (requiredVehicleType, from, to, duration) = deliveryNetwork.findRouteForCargo(cargo, cargoDestinations).findLeg { leg -> leg.from == location }!!
            if (requiredVehicleType != vehicle.type) {
                return this
            }
            val isCurrentlyWaitingForCargo = vehicleActivities.first { vehicleActivity -> vehicleActivity.vehicle == vehicle } is WaitingForCargo
            val legStarted = LegStarted(vehicle, from, to, duration)
            val events = if (isCurrentlyWaitingForCargo) listOf(VehicleStoppedWaitingForCargo(vehicle, from), legStarted) else listOf(legStarted)
            copy(vehicleActivities = replaceVehicleActivityForVehicle(DeliveringCargo(cargo, vehicle, from, to, duration)),
                    history = history.addAll(events),
                    locationBuffers = locationBuffers.pickupCargo(from))
        }
    }

    private fun replaceVehicleActivityForVehicle(vehicleActivity: VehicleActivity): PersistentList<VehicleActivity> {
        val index = vehicleActivities.indexOfFirst { va -> va.vehicle == vehicleActivity.vehicle }
        return vehicleActivities.set(index, vehicleActivity)
    }
}

private fun DeliveryNetwork.findRouteForCargo(cargo: Cargo, cargoDestinations: Map<Cargo, Destination>): Route {
    val destination = cargoDestinations[cargo]
    return network.first { route -> route.legs.any { leg -> leg.to == destination } }
}

private data class LocationBuffers(private val buffer: PersistentMap<Location, PersistentList<Cargo>> = persistentMapOf()) {
    fun dropOffCargo(cargo: Cargo, location: Location): LocationBuffers = copy(buffer = buffer.put(location, buffer.getOrDefault(location, persistentListOf()).add(cargo)))
    fun findStockedCargoAt(location: Location): Cargo? = buffer[location]?.firstOrNull()
    fun pickupCargo(location: Location): LocationBuffers = copy(buffer = buffer.put(location, buffer[location]!!.removeAt(0)))
}

fun deliverCargo(cargosToDeliver: List<Cargo>, fleet: Fleet, deliveryNetwork: DeliveryNetwork): List<DomainEvent> {

    val cargoDestinations: Map<Cargo, Destination> = cargosToDeliver.map { cargo ->
        cargo to when (cargo) {
            Cargo.A -> WarehouseA
            Cargo.B -> WarehouseB
        }
    }.toMap()

    val locationBuffers = LocationBuffers(persistentMapOf(Factory to cargosToDeliver.toPersistentList()))

    val initialOverview = DeliveryOverview(
            vehicleActivities = fleet.map { vehicle ->
                WaitingForCargo(vehicle, location = if (vehicle.type == Truck) Factory else Port)
            }.toPersistentList(),
            locationBuffers = locationBuffers,
            cargoDestinations = cargoDestinations.toImmutableMap())

    return generateSequence(initialOverview) { deliveryOverview ->
        val updatedOverview = deliveryOverview.fold(deliveryOverview) { overview, va ->
            // Move all vehicles
            when (va) {
                is WaitingForCargo -> overview.pickUpOrWaitForCargo(va.vehicle, va.location, deliveryNetwork)
                is DeliveringCargo -> {
                    val vehicleActivityAfterMove = va.move()
                    if (vehicleActivityAfterMove.hasArrived()) {
                        val deliveringCargoEvents = mutableListOf<DomainEvent>()
                        if (vehicleActivityAfterMove.to is Destination) {
                            deliveringCargoEvents.add(CargoWasDeliveredToDestination(vehicleActivityAfterMove.vehicle, vehicleActivityAfterMove.to, overview.elapsedTime))
                        }
                        deliveringCargoEvents.add(LegCompleted(va.vehicle, va.from, va.to, vehicleActivityAfterMove.elapsedTime))
                        val vehicleActivity = Returning(va.vehicle, from = va.to, to = va.from, legTime = va.legTime)
                        overview.appendHistory(deliveringCargoEvents).dropOffCargo(vehicleActivityAfterMove.cargo, va.to).updateVehicleActivity(vehicleActivity)
                    } else {
                        overview.updateVehicleActivity(vehicleActivityAfterMove)
                    }
                }
                is Returning -> {
                    val vehicleActivityAfterMove = va.move()
                    if (vehicleActivityAfterMove.hasArrived()) {
                        overview.pickUpOrWaitForCargo(va.vehicle, vehicleActivityAfterMove.to, deliveryNetwork)
                    } else {
                        overview.updateVehicleActivity(vehicleActivityAfterMove)
                    }
                }
            }
        }

        when {
            updatedOverview.completed -> null
            updatedOverview.history.count { it is CargoWasDeliveredToDestination } == cargosToDeliver.size -> updatedOverview.copy(completed = true).appendHistory(AllCargoWasDelivered(updatedOverview.elapsedTime))
            else -> updatedOverview.passTime(1)
        }
    }.last().history
}