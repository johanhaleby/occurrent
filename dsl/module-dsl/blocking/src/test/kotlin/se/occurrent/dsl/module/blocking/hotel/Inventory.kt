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

package se.occurrent.dsl.module.blocking.hotel

import se.occurrent.dsl.module.blocking.hotel.InventoryCommand.AddRoom
import java.util.*

sealed class InventoryCommand : HotelCommand {
    data class AddRoom(val hotelId: String, val roomId: UUID, val roomName: String) : InventoryCommand()
}

sealed class InventoryEvent : HotelEvent {
    data class RoomAdded(val hotelId: String, val roomId: UUID, val roomName: String) : InventoryEvent()
}

object Inventory {

    fun addRoom(previousEvents: List<HotelEvent>, cmd: AddRoom): List<HotelEvent> {
        return emptyList()
    }
}