package se.haleby.occurrent.examples.tycoon

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import se.haleby.occurrent.examples.tycoon.Cargo.A
import se.haleby.occurrent.examples.tycoon.Cargo.B
import se.haleby.occurrent.examples.tycoon.DeliveryNetwork.Companion.deliveryNetwork
import se.haleby.occurrent.examples.tycoon.DeliveryPlan.Companion.deliveryPlan
import se.haleby.occurrent.examples.tycoon.DomainEvent.AllCargoHasBeenDelivered
import se.haleby.occurrent.examples.tycoon.DomainEvent.TimeElapsed
import se.haleby.occurrent.examples.tycoon.Fleet.Companion.fleet
import se.haleby.occurrent.examples.tycoon.Location.*
import se.haleby.occurrent.examples.tycoon.VehicleType.Ship
import se.haleby.occurrent.examples.tycoon.VehicleType.Truck

class TycoonTest {

    @ParameterizedTest
    @CsvSource("A,5", "AB,5", "BB,5", "ABB,7", "AABABBAB,29", "ABBBABAAABBB,41")
    fun `history is returned when delivering cargo`(cargosToDeliverAsString: String, expectedTimeString: String) {
        // Given
        val deliveryNetwork = deliveryNetwork {
            route {
                leg(requiredVehicleType = Truck, from = Factory, to = Port, duration = 1)
                leg(requiredVehicleType = Ship, from = Port, to = WarehouseA, duration = 4)
            }
            route {
                leg(requiredVehicleType = Truck, from = Factory, to = WarehouseB, duration = 5)
            }
        }

        val fleet = fleet {
            add(vehicleName = "A", vehicleType = Truck, at = Factory)
            add(vehicleName = "B", vehicleType = Truck, at = Factory)
            add(vehicleName = "Ship", vehicleType = Ship, at = Port)
        }

        val deliveryPlan = cargosToDeliverAsString.parseToDeliveryPlan()

        // When
        val events = deliverCargo(deliveryPlan, fleet, deliveryNetwork)

        // Then
        println("==== History ===")
        println(events.joinToString(separator = "\n"))
        println("================")
        val elapsedTimeInCargoWasDeliveredToDestinationEvent = (events.first { it is AllCargoHasBeenDelivered } as AllCargoHasBeenDelivered).elapsedTime
        val calculatedTimeElapsed = events.filterIsInstance<TimeElapsed>().map(TimeElapsed::time).reduce(Int::plus)
        println("Elapsed Time (1): $elapsedTimeInCargoWasDeliveredToDestinationEvent hours")
        println("Elapsed Time (2): $calculatedTimeElapsed hours")

        assertAll(
                { assertThat(elapsedTimeInCargoWasDeliveredToDestinationEvent).isEqualTo(expectedTimeString.toInt()) },
                { assertThat(calculatedTimeElapsed).isEqualTo(expectedTimeString.toInt()) }
        )
    }
}

private fun String.parseToDeliveryPlan(): DeliveryPlan {
    val cargoDestinations = map { c ->
        when (c) {
            'A' -> A to WarehouseA
            'B' -> B to WarehouseB
            else -> throw IllegalArgumentException("Invalid cargo $c in $this")
        }
    }

    return deliveryPlan {
        cargoDestinations.forEach { (cargo, destination) ->
            deliver(cargo = cargo, from = Factory, to = destination)
        }
    }
}