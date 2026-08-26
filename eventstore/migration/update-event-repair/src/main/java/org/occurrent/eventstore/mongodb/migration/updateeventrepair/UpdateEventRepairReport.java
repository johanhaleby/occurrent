/*
 * Copyright 2026 Johan Haleby
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


package org.occurrent.eventstore.mongodb.migration.updateeventrepair;

import org.jspecify.annotations.NullMarked;

/**
 * What {@link UpdateEventRepair#report()} found, so an operator can size the damage before deciding to repair
 * anything. Producing it writes nothing.
 *
 * @param eventsNeedingRepair  How many stored events the repair would touch. {@code 0} means no damage this tool
 *                             can detect, which is not the same as no damage. An update that dropped both
 *                             {@code position} and the {@code dcbtags} extension leaves a document that matches
 *                             neither query, whether the event was a DCB append or a plain stream event, and
 *                             {@link UpdateEventRepair} describes both cases.
 * @param eventsWithLostPosition How many stored events have DCB tags but no {@code position} at all. The repair
 *                             cannot restore a position that was never stored, so this count is what survives a
 *                             completed run and it is not a subset of {@code eventsNeedingRepair}. Once such an
 *                             event has had its tag array rebuilt it no longer needs repair, and it is still
 *                             counted here. See {@link UnrecoverableEvent.Reason#POSITION_LOST}.
 */
@NullMarked
public record UpdateEventRepairReport(long eventsNeedingRepair, long eventsWithLostPosition) {
}
