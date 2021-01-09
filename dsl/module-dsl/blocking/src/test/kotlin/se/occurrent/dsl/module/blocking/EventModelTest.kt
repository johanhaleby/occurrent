/*
 * Copyright 2021 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package se.occurrent.dsl.module.blocking

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.generic.GenericCloudEventConverter
import org.occurrent.domain.DomainEventConverter
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel
import se.occurrent.dsl.module.blocking.em.eventModel
import se.occurrent.dsl.module.blocking.hotel.*
import se.occurrent.dsl.module.blocking.hotel.InventoryCommand.AddRoom
import se.occurrent.dsl.module.blocking.hotel.InventoryEvent.RoomAdded
import java.util.concurrent.CopyOnWriteArrayList


class EventModelTest {

    fun `event model example`() {
        // Given
        val cloudEventConverter : CloudEventConverter<HotelEvent> = GenericCloudEventConverter({ e -> TODO()}, { e -> TODO()})
        val subscriptionModel = InMemorySubscriptionModel()
        val eventStore = InMemoryEventStore(subscriptionModel)

        val roomAvailability = CopyOnWriteArrayList<String>()

        eventModel<HotelCommand, HotelEvent>(eventStore, cloudEventConverter) {
            // Two different slices:
            // ui->command->event (in the event store)
            // event (from ES) -> handler - RM - UI
            swimlane("Inventory") {
                slice(actor = "Manager", name = "Add Room") {
                    wireframe("some ui").
                    command(AddRoom::hotelId, Inventory::addRoom)
                }
                slice(actor ="Manager", name= "Update Room Availability") {
//                    whenever<RoomAdded>().
//                    updateReadModel("Room Availability") { roomAdded ->
//                        roomAvailability.add(roomAdded.roomName)
//                    }.
//                    usedBy(wireframe="fkds")
                }
            }
        }
    }
}