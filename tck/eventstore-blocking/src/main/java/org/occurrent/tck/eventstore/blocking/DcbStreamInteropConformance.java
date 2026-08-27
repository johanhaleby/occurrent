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
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.filter.Filter;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.tck.ConformanceEvents.event;
import static org.occurrent.tck.ConformanceEvents.idsOf;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.*;

/**
 * What a store owes when it has both capabilities at once. DCB reads see DCB events and nothing else, while the
 * general query API sees everything, because both modes share one CloudEvent store.
 * <p>
 * This is separate from {@link DcbEventStoreConformance} rather than folded into it because these assertions need
 * {@link EventStoreCapability#STREAM} as well as {@link EventStoreCapability#DCB}. Every store shipping with Occurrent
 * has both, and DCB is documented as a capability over shared CloudEvent storage, so in practice everything extends
 * this. Requiring the pair here anyway keeps the DCB suite itself runnable by a DCB-only store, and keeps the reason a
 * store is not running these assertions a visible missing subclass rather than a skip.
 * <p>
 * Nothing here asserts which storage stream a DCB event landed in. A store derives placement from tags and that is
 * explicitly not part of the DCB contract, so these assertions reach DCB events through the query API and through
 * their tags, never through a stream id.
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("a store with both DCB and stream capabilities")
public abstract class DcbStreamInteropConformance extends EventStoreConformance {

    private static final String DEFINED = "NameDefined";
    private static final String CHANGED = "NameChanged";
    private static final String NAME_1 = "name:1";

    @Override
    protected final Set<EventStoreCapability> requiredCapabilities() {
        return Set.of(EventStoreCapability.STREAM, EventStoreCapability.DCB);
    }

    @Test
    void a_dcb_read_does_not_return_a_stream_written_event() {
        eventStore().write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("A", DEFINED)));

        assertThat(dcbEventStore().read(DcbCriteria.all()).events())
                .as("MatchAll means every DCB event, not every event: a stream write carries no DCB tags extension, "
                        + "which is the discriminator, so it must stay invisible to a DCB read")
                .isEmpty();
        assertThat(dcbEventStore().read(DcbCriteria.type(DEFINED)).events())
                .as("A type-scoped criteria must not reach a stream-written event that happens to have that type")
                .isEmpty();
    }

    @Test
    void dcb_exists_and_count_do_not_see_a_stream_written_event() {
        eventStore().write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("A", DEFINED)));

        assertThat(dcbEventStore().exists(DcbCriteria.all()))
                .as("A store holding only stream-written events holds no DCB events").isFalse();
        assertThat(dcbEventStore().count(DcbCriteria.all()))
                .as("A store holding only stream-written events counts no DCB events").isZero();
    }

    @Test
    void a_dcb_append_is_readable_through_the_general_query_api() {
        dcbEventStore().append(List.of(taggedEventWithId("A", DEFINED, NAME_1)));

        assertThat(idsOf(queries().query(Filter.all())))
                .as("DCB and stream mode share one CloudEvent store, so a DCB append must be an ordinary CloudEvent "
                        + "to everything that reads the store, including projections and subscriptions")
                .containsExactly("A");
    }

    @Test
    void a_stream_write_refuses_an_event_carrying_dcb_tags() {
        CloudEvent taggedEvent = taggedEventWithId("A", DEFINED, NAME_1);

        assertThatThrownBy(() -> eventStore()
                .write("stream:1", WriteCondition.anyStreamVersion(), List.of(taggedEvent)))
                .as("The DCB tags extension is what tells the two modes apart, so a stream write must refuse to "
                        + "stamp it rather than create an event that reads as DCB but was never placed by tags")
                .isExactlyInstanceOf(IllegalArgumentException.class)
                // Asserted verbatim rather than loosely, because all four stores word it identically today while
                // building it independently, so a store drifting away from it is worth a failing test.
                .hasMessage("A DCB-tagged event cannot be written through the stream write(...) API, use the DCB "
                        + "append(...) API instead.");

        assertThat(queries().count(Filter.all()))
                .as("The refused write must have written nothing").isZero();
    }

    @Test
    void a_dcb_append_and_a_stream_write_share_one_position_sequence() {
        // A stream write bracketed by DCB appends, so the sequence is checked in both directions. Writing only
        // stream-then-DCB would leave the other half unproven, which is a store advancing its position past a DCB
        // append when a stream write follows one.
        DcbAppendResult firstAppend = dcbEventStore().append(List.of(taggedEventWithId("A", DEFINED, NAME_1)));
        eventStore().write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("B", DEFINED)));
        DcbAppendResult lastAppend = dcbEventStore().append(List.of(taggedEventWithId("C", CHANGED, NAME_1)));

        // Read back through the position-ordered reader, which is the one view over both modes at once. Nothing here
        // asserts a literal position or that any two are contiguous: a store reserving position blocks outside its
        // write transaction can leave a gap between any two writes (ADR 84).
        assertThat(idsOf(positionOrderedReader().readInPositionOrder(Filter.all(), PositionRange.fromBeginning())))
                .as("One global position sequence covers both modes, so writes must come back in the order they "
                        + "happened rather than grouped by mode, whichever mode wrote first")
                .containsExactly("A", "B", "C");
        assertThat(lastAppend.firstSequencePosition())
                .as("A DCB append after a stream write must be assigned a strictly higher position, so the stream "
                        + "write advanced the same counter the DCB append draws from")
                .isGreaterThan(firstAppend.lastSequencePosition());
        assertThat(positionOrderedReader().currentPosition())
                .as("currentPosition() is a high-watermark over the whole store, so it must have reached the "
                        + "position the last DCB append reported")
                .isGreaterThanOrEqualTo(lastAppend.lastSequencePosition());
    }

    @Test
    void a_no_token_condition_means_currently_exists_rather_than_ever_appended() {
        DcbCriteria criteria = DcbCriteria.tags(tag(NAME_1));
        CloudEvent existing = taggedEventWithId("A", DEFINED, NAME_1);
        dcbEventStore().append(List.of(existing));

        assertThatThrownBy(() -> dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_1)), failIfEventsMatch(criteria)))
                .as("While a matching event exists the untokenized guard must conflict")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);

        operations().deleteEvent(existing.getId(), existing.getSource());

        // This is the assertion that needs the operations capability, and the reason this suite exists. A store
        // answering the untokenized guard from per-boundary version counters would still conflict here, because such
        // counters are never decremented. The contract is about the events, so it must survive a delete.
        DcbAppendResult result = dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_1)), failIfEventsMatch(criteria));

        assertThat(result.eventCount())
                .as("Without a token the condition asks whether a matching event exists now, not whether one was "
                        + "ever appended, so deleting the match must let the append through")
                .isEqualTo(1);
        assertThat(typesOf(dcbEventStore().read(criteria).events()))
                .as("Only the newly appended event may remain in the boundary")
                .containsExactly(CHANGED);
    }

    @Test
    void updating_a_dcb_event_with_a_fresh_replacement_event_keeps_its_own_tags_and_position() {
        dcbEventStore().append(List.of(taggedEventWithId("A", DEFINED, NAME_1)));
        CloudEvent original = queries().query(Filter.id("A")).findFirst().orElseThrow();
        var originalTags = DcbCloudEvents.getTags(original);
        long originalPosition = OccurrentCloudEventExtension.getPosition(original);

        // A fresh event built from scratch carries different tags and no position of its own. Both are
        // store-owned, so the update must not move the event across the consistency boundary its tags defined,
        // and must not drop it from a DCB read that filters on position, which is every DCB read.
        CloudEvent updated = operations().updateEvent("A", original.getSource(),
                original2 -> taggedEventWithId("A", CHANGED, "other:9")).orElseThrow();

        CloudEvent stored = queries().query(Filter.id("A")).findFirst().orElseThrow();
        assertAll(
                () -> assertThat(DcbCloudEvents.getTags(updated)).isEqualTo(originalTags),
                () -> assertThat(DcbCloudEvents.getTags(stored)).isEqualTo(originalTags),
                () -> assertThat(OccurrentCloudEventExtension.getPosition(updated)).isEqualTo(originalPosition),
                () -> assertThat(OccurrentCloudEventExtension.getPosition(stored)).isEqualTo(originalPosition),
                () -> assertThat(typesOf(dcbEventStore().read(DcbCriteria.tags(tag(NAME_1))).events()))
                        .as("A dropped position would filter the updated event out of every DCB read, since "
                                + "position(event) > afterPosition compares against zero, not just fail to match "
                                + "the tag")
                        .containsExactly(CHANGED)
        );
    }
}
