package se.haleby.occurrent.examples.tycoon

import org.junit.jupiter.api.Test
import se.haleby.occurrent.examples.tycoon.Cargo.A
import se.haleby.occurrent.examples.tycoon.Cargo.B
import se.haleby.occurrent.examples.tycoon.DomainEvent.CargoWasDeliveredToDestination
import se.haleby.occurrent.examples.tycoon.DomainEvent.TimePassed


class TycoonTest {

    @Test
    fun `prints history and elapsed time`() {
        // When
        val events = deliverCargo(listOf(A, B, B, B, A, B, A, A, A, B, B, B), fleet, deliveryNetwork)

        // Then
        println("==== History ===")
        println(events.joinToString(separator = "\n"))
        println("================")
        println("Elapsed Time (1): " + events.filterIsInstance<CargoWasDeliveredToDestination>().last().elapsedTime + " hours")
        println("Elapsed Time (2): " + events.filterIsInstance<TimePassed>().map(TimePassed::time).reduce(Int::plus) + " hours")

    }
}