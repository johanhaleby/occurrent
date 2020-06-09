package se.haleby.occurrent.examples.tycoon

import org.junit.jupiter.api.Test
import se.haleby.occurrent.examples.tycoon.Cargo.A
import se.haleby.occurrent.examples.tycoon.Cargo.B
import se.haleby.occurrent.examples.tycoon.DomainEvent.CargoWasDeliveredToDestination
import se.haleby.occurrent.examples.tycoon.DomainEvent.TimePassed


class TycoonTest {

    @Test
    fun `prints history and elapsed time`() {
        // Given

        // Legs
        val factoryToPort = Leg(requiredVehicleType = VehicleType.Truck, from = StartingPoint.Factory, to = TransitPoint.Port, duration = 1)
        val portToWarehouseA = Leg(requiredVehicleType = VehicleType.Ship, from = TransitPoint.Port, to = Destination.WarehouseA, duration = 4)
        val factoryToWarehouseB = Leg(requiredVehicleType = VehicleType.Truck, from = StartingPoint.Factory, to = Destination.WarehouseB, duration = 5)

        // Routes
        val routeFromFactoryToWarehouseA = Route(factoryToPort, portToWarehouseA)
        val routeFromFactoryToWarehouseB = Route(factoryToWarehouseB)

        // Delivery network
        val deliveryNetwork = DeliveryNetwork(routeFromFactoryToWarehouseA, routeFromFactoryToWarehouseB)

        // Fleet
        val fleet = Fleet(Vehicle(name = "A", type = VehicleType.Truck), Vehicle(name = "B", type = VehicleType.Truck), Vehicle(name = "Ship", type = VehicleType.Ship))


        // When
        val events = deliverCargo(listOf(A, A, B, A, B, B, A, B), fleet, deliveryNetwork)

        // Then
        println("==== History ===")
        println(events.joinToString(separator = "\n"))
        println("================")
        println("Elapsed Time (1): " + events.filterIsInstance<CargoWasDeliveredToDestination>().last().elapsedTime + " hours")
        println("Elapsed Time (2): " + events.filterIsInstance<TimePassed>().map(TimePassed::time).reduce(Int::plus) + " hours")

    }
}