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

package org.occurrent.tck.eventstore.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.dcb.DcbAppendConfirmation;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCriteria;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.tag;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.taggedEventWithId;

/**
 * When a store confirms that a DCB append is committed, which a snapshot waits for before it saves the append's
 * marker versions.
 * <p>
 * This is separate from {@link DcbMarkerVersionsConformance} because the case of deleted events needs
 * {@link EventStoreCapability#STREAM} for {@code deleteEvent}, as {@link DcbStreamInteropConformance} does. A store
 * that does not implement {@link DcbAppendConfirmation} fails every test here, since it is treated as unconfirmed and
 * a snapshot over it is never saved, which is a contract this suite is there to hold a store to.
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the DCB append confirmation")
public abstract class DcbAppendConfirmationConformance extends EventStoreConformance {

    private static final String DEPOSITED = "Deposited";
    private static final String ACCOUNT = "account:a1";

    @Override
    protected final Set<EventStoreCapability> requiredCapabilities() {
        return Set.of(EventStoreCapability.STREAM, EventStoreCapability.DCB);
    }

    @Test
    void confirms_the_result_of_an_unconditional_append_that_is_committed() {
        DcbAppendResult result = dcbEventStore().append(List.of(taggedEventWithId("a", DEPOSITED, ACCOUNT)));

        assertThat(confirmation().isConfirmed(result)).isTrue();
    }

    @Test
    void confirms_the_result_of_a_conditional_append_that_is_committed() {
        DcbCriteria boundary = DcbCriteria.tags(tag(ACCOUNT));
        DcbAppendResult result = dcbEventStore().append(List.of(taggedEventWithId("a", DEPOSITED, ACCOUNT)),
                failIfEventsMatch(dcbEventStore().read(boundary).consistencyToken()));

        assertThat(confirmation().isConfirmed(result)).isTrue();
    }

    @Test
    void confirms_the_result_of_an_append_of_several_events() {
        DcbAppendResult result = dcbEventStore().append(List.of(
                taggedEventWithId("a", DEPOSITED, ACCOUNT), taggedEventWithId("b", DEPOSITED, ACCOUNT), taggedEventWithId("c", DEPOSITED, ACCOUNT)));

        assertThat(confirmation().isConfirmed(result)).isTrue();
    }

    @Test
    void confirms_the_result_of_an_append_followed_by_a_later_append() {
        DcbAppendResult first = dcbEventStore().append(List.of(taggedEventWithId("a", DEPOSITED, ACCOUNT)));
        dcbEventStore().append(List.of(taggedEventWithId("b", DEPOSITED, ACCOUNT)));

        assertThat(confirmation().isConfirmed(first)).isTrue();
    }

    @Test
    void does_not_confirm_a_result_that_has_no_append_id() {
        DcbAppendResult committed = dcbEventStore().append(List.of(taggedEventWithId("a", DEPOSITED, ACCOUNT)));
        DcbAppendResult withoutAppendId = new DcbAppendResult(committed.firstSequencePosition(), committed.lastSequencePosition(), committed.eventCount());

        assertThat(confirmation().isConfirmed(committed)).as("The same positions with their append id are confirmed, or this proves nothing").isTrue();
        assertThat(withoutAppendId.appendId()).isEmpty();
        assertThat(confirmation().isConfirmed(withoutAppendId)).isFalse();
    }

    @Test
    void does_not_confirm_a_result_whose_append_id_belongs_to_no_event() {
        DcbAppendResult committed = dcbEventStore().append(List.of(taggedEventWithId("a", DEPOSITED, ACCOUNT)));
        DcbAppendResult ofAnotherAppend = new DcbAppendResult(committed.firstSequencePosition(), committed.lastSequencePosition(),
                committed.eventCount(), Optional.of(new AppendId(UUID.randomUUID())));

        assertThat(confirmation().isConfirmed(ofAnotherAppend)).isFalse();
    }

    @Test
    void does_not_confirm_a_result_whose_events_were_deleted() {
        CloudEvent event = taggedEventWithId("a", DEPOSITED, ACCOUNT);
        DcbAppendResult result = dcbEventStore().append(List.of(event));
        assertThat(confirmation().isConfirmed(result)).as("It is confirmed before the delete, or this proves nothing").isTrue();

        operations().deleteEvent(event.getId(), event.getSource());

        assertThat(confirmation().isConfirmed(result)).isFalse();
    }

    @Test
    void confirms_the_result_of_an_append_of_several_events_while_one_of_them_remains() {
        CloudEvent deleted = taggedEventWithId("a", DEPOSITED, ACCOUNT);
        DcbAppendResult result = dcbEventStore().append(List.of(deleted, taggedEventWithId("b", DEPOSITED, ACCOUNT)));

        operations().deleteEvent(deleted.getId(), deleted.getSource());

        assertThat(confirmation().isConfirmed(result)).as("An event of the append is still there, which shows the append committed").isTrue();
    }

    private DcbAppendConfirmation confirmation() {
        if (dcbEventStore() instanceof DcbAppendConfirmation confirmation) {
            return confirmation;
        }
        throw new AssertionError("The DCB event store must implement DcbAppendConfirmation, or a snapshot over it is never saved. "
                + dcbEventStore().getClass().getName() + " does not");
    }
}
