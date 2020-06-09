package se.haleby.occurrent.examples.tycoon

import kotlinx.collections.immutable.*
import se.haleby.occurrent.examples.tycoon.Destination.WarehouseA
import se.haleby.occurrent.examples.tycoon.Destination.WarehouseB
import se.haleby.occurrent.examples.tycoon.DomainEvent.*
import se.haleby.occurrent.examples.tycoon.StartingPoint.Factory
import se.haleby.occurrent.examples.tycoon.TransitPoint.Port
import se.haleby.occurrent.examples.tycoon.VehicleActivity.EnRouteVehicleActivity
import se.haleby.occurrent.examples.tycoon.VehicleActivity.EnRouteVehicleActivity.DeliveringCargo
import se.haleby.occurrent.examples.tycoon.VehicleActivity.EnRouteVehicleActivity.Returning
import se.haleby.occurrent.examples.tycoon.VehicleActivity.WaitingForCargo
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
    data class AllCargoHasBeenDelivered(val elapsedTime: Hours) : DomainEvent()
    data class TimePassed(val time: Hours) : DomainEvent()
}

// Use cases
fun deliverCargo(cargosToDeliver: List<Cargo>, fleet: Fleet, deliveryNetwork: DeliveryNetwork): List<DomainEvent> {

    val cargoDestinations: ImmutableMap<Cargo, Destination> = cargosToDeliver.map { cargo ->
        cargo to when (cargo) {
            Cargo.A -> WarehouseA
            Cargo.B -> WarehouseB
        }
    }.toMap().toImmutableMap()

    val cargoAtLocation = CargoAtLocation(persistentMapOf(Factory to cargosToDeliver.toPersistentList()))

    // TODO This should probably be passed in a parameter
    val vehicleActivities = fleet.map { vehicle ->
        vehicle to WaitingForCargo(location = if (vehicle.type == Truck) Factory else Port)
    }.toMap().toPersistentMap()

    val scenario = Scenario(
            vehicleActivities = vehicleActivities,
            cargoAtLocation = cargoAtLocation,
            cargosToDeliver = cargosToDeliver,
            deliveryNetwork = deliveryNetwork,
            cargoDestinations = cargoDestinations
    )

    return execute(scenario).history
}

// Internal
private tailrec fun execute(scenario: Scenario): Scenario {
    val updatedScenario = scenario.proceed { vehicle, activity ->
        when (activity) {
            is WaitingForCargo -> loadOrWaitForCargo(vehicle, activity.location, activity)
            is EnRouteVehicleActivity -> continueRoute(vehicle, activity)
        }
    }

    return if (updatedScenario.isScenarioCompleted()) {
        updatedScenario
    } else {
        execute(updatedScenario.passTime(1))
    }
}

private sealed class VehicleActivity {

    sealed class EnRouteVehicleActivity : VehicleActivity() {
        abstract val from: Location
        abstract val to: Location
        abstract val elapsedTime: Hours
        abstract val legTime: Hours

        val remainingTime: Hours get() = legTime - elapsedTime
        fun hasArrived(): Boolean = remainingTime == 0

        data class DeliveringCargo(val cargo: Cargo, override val from: Location, override val to: Location, override val legTime: Hours, override val elapsedTime: Hours = 0) : EnRouteVehicleActivity() {
            fun continueRoute(): DeliveringCargo = copy(elapsedTime = elapsedTime.inc())
        }

        data class Returning(override val from: Location, override val to: Location, override val legTime: Hours, override val elapsedTime: Hours = 0) : EnRouteVehicleActivity() {
            fun continueRoute(): Returning = copy(elapsedTime = elapsedTime.inc())
        }
    }

    data class WaitingForCargo(val location: Location) : VehicleActivity()
}

private data class Scenario(val vehicleActivities: PersistentMap<Vehicle, VehicleActivity>,
                            val elapsedTime: Hours = 0,
                            val cargoDestinations: ImmutableMap<Cargo, Destination>,
                            val cargoAtLocation: CargoAtLocation = CargoAtLocation(),
                            val history: PersistentList<DomainEvent> = persistentListOf(),
                            val cargosToDeliver: List<Cargo>,
                            val deliveryNetwork: DeliveryNetwork) {
    fun loadOrWaitForCargo(vehicle: Vehicle, location: Location, currentVehicleActivity: VehicleActivity): Scenario {
        val cargo = cargoAtLocation.findStockedCargoAt(location)
        return if (cargo == null && currentVehicleActivity !is WaitingForCargo) {
            copy(vehicleActivities = vehicleActivities.put(vehicle, WaitingForCargo(location)),
                    history = history.add(VehicleStartedWaitingForCargo(vehicle, location)))
        } else if (cargo == null) {
            this
        } else {
            val (requiredVehicleType, from, to, duration) = deliveryNetwork.findRouteForCargo(cargo, cargoDestinations).findLeg { leg -> leg.from == location }!!
            if (requiredVehicleType != vehicle.type) {
                return this
            }
            val isCurrentlyWaitingForCargo = vehicleActivities[vehicle] is WaitingForCargo
            val legStarted = LegStarted(vehicle, from, to, duration)
            val events = if (isCurrentlyWaitingForCargo) listOf(VehicleStoppedWaitingForCargo(vehicle, from), legStarted) else listOf(legStarted)
            copy(vehicleActivities = vehicleActivities.put(vehicle, DeliveringCargo(cargo, from, to, duration)),
                    history = history.addAll(events),
                    cargoAtLocation = cargoAtLocation.loadCargo(from))
        }
    }

    fun continueRoute(vehicle: Vehicle, vehicleActivity: EnRouteVehicleActivity): Scenario = when (vehicleActivity) {
        is DeliveringCargo -> {
            val vehicleActivityAfterRouteWasContinued = vehicleActivity.continueRoute()
            if (vehicleActivityAfterRouteWasContinued.hasArrived()) {
                val currentLocation = vehicleActivity.to
                val deliveringCargoEvents = mutableListOf<DomainEvent>()
                if (vehicleActivityAfterRouteWasContinued.to is Destination) {
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

    fun proceed(fn: Scenario.(vehicle: Vehicle, activity: VehicleActivity) -> Scenario): Scenario = vehicleActivities.entries.fold(this) { s, (vehicle, activity) ->
        fn(s, vehicle, activity)
    }

    fun isScenarioCompleted(): Boolean = history.any { it is AllCargoHasBeenDelivered }

    // private helpers

    fun passTime(time: Hours) = copy(elapsedTime = elapsedTime + time, history = history.add(TimePassed(time)))
    private fun appendHistory(event: DomainEvent): Scenario = copy(history = history.add(event))
    private fun appendHistory(events: List<DomainEvent>): Scenario = copy(history = history.addAll(events))

    private fun updateVehicleActivity(vehicle: Vehicle, vehicleActivity: VehicleActivity): Scenario {
        return copy(vehicleActivities = vehicleActivities.put(vehicle, vehicleActivity))
    }

    private fun unloadCargo(cargo: Cargo, location: Location): Scenario {
        val s = if (history.count { it is CargoWasDeliveredToDestination } == cargosToDeliver.size) {
            appendHistory(AllCargoHasBeenDelivered(elapsedTime))
        } else {
            this
        }
        return s.copy(cargoAtLocation = cargoAtLocation.unloadCargo(cargo, location))
    }
}

private fun DeliveryNetwork.findRouteForCargo(cargo: Cargo, cargoDestinations: Map<Cargo, Destination>): Route {
    val destination = cargoDestinations[cargo]
    return network.first { route -> route.legs.any { leg -> leg.to == destination } }
}

private data class CargoAtLocation(private val buffer: PersistentMap<Location, PersistentList<Cargo>> = persistentMapOf()) {
    fun unloadCargo(cargo: Cargo, location: Location): CargoAtLocation = copy(buffer = buffer.put(location, buffer.getOrDefault(location, persistentListOf()).add(cargo)))
    fun findStockedCargoAt(location: Location): Cargo? = buffer[location]?.firstOrNull()
    fun loadCargo(location: Location): CargoAtLocation = copy(buffer = buffer.put(location, buffer[location]!!.removeAt(0)))
}