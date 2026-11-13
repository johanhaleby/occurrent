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

import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.DcbSnapshotInvalidException;
import org.occurrent.eventstore.api.dcb.internal.DcbMarkerVersionsSupport;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.tck.ConformanceEvents.idsOf;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.tag;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.taggedEvent;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.taggedEventWithId;

/**
 * What a store that keeps a version marker per tag and type answers when a marker is deleted after a read took its
 * token. A store with no markers has nothing to delete and doesn't run this suite.
 * <p>
 * An implementation says how to delete a marker and how to delete events, and the suite deletes events and then the
 * marker, the way the pruning rule has an operator do it in one transaction (ADR 149). A marker key is
 * {@code tag:<tag>} for a tag. A type has one marker per stripe, and the suite takes their keys from
 * {@link DcbMarkerVersionsSupport}, the way {@link DcbMarkerVersionsConformance} does. The suite states what a
 * token or saved versions read before the deletion get after a later append recreated the marker, which holds whichever
 * way a store recreates it. The rule only says a stripe of a type may be deleted in a transaction that also finds no
 * event of the type.
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the DCB marker prune contract")
public abstract class DcbMarkerPruneConformance extends EventStoreConformance {

    private static final String DEPOSITED = "Deposited";
    private static final String WITHDRAWN = "Withdrawn";
    private static final String ACCOUNT_A = "account:a1";
    private static final String ACCOUNT_B = "account:b1";
    private static final String KEY_A = "tag:account:a1";

    /**
     * Deletes the marker with this key from the store under test, the way an operator would. Called only after the
     * events the marker stands for are deleted, except in the one test that says otherwise.
     */
    protected abstract void deleteMarker(String markerKey);

    /**
     * Deletes the events with these ids from the store under test.
     */
    protected abstract void deleteEvents(String... eventIds);

    @Override
    protected final Set<EventStoreCapability> requiredCapabilities() {
        return Set.of(EventStoreCapability.DCB);
    }

    @Test
    void refuses_a_token_read_before_its_marker_and_its_event_were_deleted_when_an_append_has_recreated_the_marker() {
        DcbCriteria criteria = DcbCriteria.tags(tag(ACCOUNT_A));
        dcbEventStore().append(List.of(taggedEventWithId("first", DEPOSITED, ACCOUNT_A)));
        DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();
        deleteEvents("first");
        deleteMarker(KEY_A);
        dcbEventStore().append(List.of(taggedEventWithId("second", DEPOSITED, ACCOUNT_A)));

        assertThatThrownBy(() -> dcbEventStore().append(List.of(taggedEvent(WITHDRAWN, ACCOUNT_B)), failIfEventsMatch(token)))
                .as("The recreated marker is at the version the token saw, but it is a new marker, and the event that "
                        + "recreated it was committed after the read. The token must not pass for a marker it never saw")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);

        assertThat(idsOf(dcbEventStore().read(DcbCriteria.all()).events()))
                .as("The refused append must not have written its event")
                .containsExactly("second");
    }

    @Test
    void refuses_a_token_when_a_deleted_marker_and_another_marker_that_was_absent_at_the_read_add_up_to_the_versions_it_saw() {
        DcbCriteria criteria = DcbCriteria.anyOf(DcbCriteria.tags(tag(ACCOUNT_A)), DcbCriteria.tags(tag(ACCOUNT_B)));
        dcbEventStore().append(List.of(taggedEventWithId("a-1", DEPOSITED, ACCOUNT_A)));
        dcbEventStore().append(List.of(taggedEventWithId("a-2", DEPOSITED, ACCOUNT_A)));
        DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();
        deleteEvents("a-1", "a-2");
        deleteMarker(KEY_A);
        dcbEventStore().append(List.of(taggedEventWithId("b-1", WITHDRAWN, ACCOUNT_B)));
        dcbEventStore().append(List.of(taggedEventWithId("b-2", WITHDRAWN, ACCOUNT_B)));

        assertThatThrownBy(() -> dcbEventStore().append(List.of(taggedEvent(DEPOSITED, "account:c1")), failIfEventsMatch(token)))
                .as("The read saw the first marker at version 2 and the second not at all, and now the second is at "
                        + "version 2 and the first is gone. Each marker is compared by itself, so a total that stayed "
                        + "the same must not let the token through")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    @Test
    void refuses_a_token_read_before_its_marker_was_deleted_while_its_event_remained_when_an_append_has_recreated_the_marker() {
        DcbCriteria criteria = DcbCriteria.type(DEPOSITED).tags(tag(ACCOUNT_A));
        dcbEventStore().append(List.of(taggedEventWithId("first", DEPOSITED, ACCOUNT_A)));
        DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();
        deleteMarker(KEY_A);
        dcbEventStore().append(List.of(taggedEventWithId("second", DEPOSITED, ACCOUNT_A)));

        assertThatThrownBy(() -> dcbEventStore().append(List.of(taggedEvent(WITHDRAWN, ACCOUNT_B)), failIfEventsMatch(token)))
                .as("The marker was deleted while the event it stands for remained, which the pruning rule doesn't "
                        + "allow. An operator who does it anyway must still not get a token through that was read "
                        + "before the marker was recreated by a later event")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    @Test
    void refuses_a_token_read_before_every_stripe_of_a_type_and_its_event_were_deleted_when_an_append_has_recreated_a_stripe() {
        DcbCriteria criteria = DcbCriteria.type(DEPOSITED);
        dcbEventStore().append(List.of(taggedEventWithId("first", DEPOSITED)));
        DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();
        deleteEvents("first");
        deleteAllStripesOf(DEPOSITED);
        dcbEventStore().append(List.of(taggedEventWithId("second", DEPOSITED)));

        assertThatThrownBy(() -> dcbEventStore().append(List.of(taggedEvent(WITHDRAWN, ACCOUNT_B)), failIfEventsMatch(token)))
                .as("The stripe the second event wrote is a new marker, and the event that created it was committed after "
                        + "the read. The token must not pass for a marker it never saw, whichever stripes the two events used")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    @Test
    void refuses_a_read_after_versions_saved_before_every_stripe_of_a_type_and_its_event_were_deleted_when_an_append_has_recreated_a_stripe() {
        DcbCriteria criteria = DcbCriteria.type(DEPOSITED);
        dcbEventStore().append(List.of(taggedEventWithId("first", DEPOSITED)));
        DcbMarkerVersions saved = dcbEventStore().read(criteria, DcbReadOptions.fromBeginning().withVersions()).markerVersions().orElseThrow();
        deleteEvents("first");
        deleteAllStripesOf(DEPOSITED);
        dcbEventStore().append(List.of(taggedEventWithId("second", DEPOSITED)));

        assertThatThrownBy(() -> dcbEventStore().read(criteria, DcbReadOptions.afterVersions(saved)))
                .as("The stripe the first event wrote was deleted and the stripe of the second may be the same one at the "
                        + "same version, but it is a new marker. A read after the saved versions would skip the second "
                        + "event if it took them for the old marker's")
                .isExactlyInstanceOf(DcbSnapshotInvalidException.class);
    }

    @Test
    void reads_the_event_appended_after_a_stripe_that_was_absent_at_the_save_was_created_deleted_with_its_event_and_created_again() {
        DcbCriteria criteria = DcbCriteria.type(DEPOSITED);
        DcbMarkerVersions saved = dcbEventStore().read(criteria, DcbReadOptions.fromBeginning().withVersions()).markerVersions().orElseThrow();
        dcbEventStore().append(List.of(taggedEventWithId("first", DEPOSITED)));
        deleteEvents("first");
        deleteAllStripesOf(DEPOSITED);
        dcbEventStore().append(List.of(taggedEventWithId("second", DEPOSITED)));

        assertThatCode(() -> dcbEventStore().read(criteria, DcbReadOptions.afterVersions(saved)))
                .as("Every stripe was absent at the save, and the one that exists now was created after it")
                .doesNotThrowAnyException();
        assertThat(idsOf(dcbEventStore().read(criteria, DcbReadOptions.afterVersions(saved)).events()))
                .as("The first event and the stripe it created are gone, so the second is the only event after the save")
                .containsExactly("second");
    }

    private void deleteAllStripesOf(String type) {
        for (int stripe = 0; stripe < DcbMarkerVersionsSupport.STRIPES; stripe++) {
            deleteMarker(DcbMarkerVersionsSupport.stripeKey(type, stripe));
        }
    }
}
