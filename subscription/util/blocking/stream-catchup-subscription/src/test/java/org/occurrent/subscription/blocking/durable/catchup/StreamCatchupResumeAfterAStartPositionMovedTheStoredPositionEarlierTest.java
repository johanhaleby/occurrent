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

package org.occurrent.subscription.blocking.durable.catchup;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.StartRefusalReportingSubscriptions.StartRefusalListener;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.subscription.internal.CatchupCheckpoints;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A resume at a position whose save the checkpoint storage refuses, since another node's start position moved the
 * stored position earlier, stores this node's version with the stored checkpoint, which takes the refusal away, and
 * writes the position once more, instead of being refused on every try.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class StreamCatchupResumeAfterAStartPositionMovedTheStoredPositionEarlierTest {

    private static final String ID = "subscription";
    private static final long VERSION = 5;
    private static final Checkpoint LATER_TOKEN = new StringBasedCheckpoint("later-token");

    private final CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<CatchupCannotResumeException> heldPaused = new CopyOnWriteArrayList<>();
    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private InMemoryEventStore eventStore;
    private StorageThatRefusesOnceAStartMovedItEarlier storage;
    private StreamCatchupSubscriptionModel catchup;

    @BeforeEach
    void create_instances() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        LocalDateTime time = LocalDateTime.now();
        List.of(nameDefined(time, "event1"), nameDefined(time, "event2"), nameDefined(time, "event3")).forEach(this::write);
        storage = new StorageThatRefusesOnceAStartMovedItEarlier();
    }

    @AfterEach
    void shutdown() {
        if (catchup != null) {
            catchup.shutdown();
        }
        inMemorySubscriptionModel.shutdown();
    }

    @Test
    void a_resume_at_a_position_after_a_start_position_moved_the_stored_position_earlier_stores_this_nodes_version_with_the_stored_checkpoint_and_then_the_position() {
        // Given a held takeover replay with a catch-up position stored with this node's version, that another node's start position then moved earlier
        heldTakeoverReplayWithACatchupPositionStored();
        Checkpoint resumedAt = new StringBasedCheckpoint(CatchupCheckpoints.format(2, 1, 3, LATER_TOKEN));
        storage.anotherStartMovedTheStoredPositionEarlier();

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.checkpoint(resumedAt)));

        // Then
        assertAll(
                () -> assertThat(thrown).as("thrown by the resume").isNull(),
                () -> assertThat(storage.refusedSaves).as("saves the storage refused").hasValue(1),
                () -> assertThat(storage.read(ID).asString()).as("stored position").isEqualTo(resumedAt.asString()),
                () -> assertThat(storage.writeVersion(ID)).as("stored version").hasValue(VERSION),
                () -> assertThat(storage.movedEarlier).as("stored position marked as moved earlier").isFalse()
        );
    }

    @Test
    void a_resume_at_a_position_stores_it_without_a_second_write_when_no_start_position_moved_the_stored_position_earlier() {
        // Given
        heldTakeoverReplayWithACatchupPositionStored();
        Checkpoint resumedAt = new StringBasedCheckpoint(CatchupCheckpoints.format(2, 1, 3, LATER_TOKEN));

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.checkpoint(resumedAt)));

        // Then
        assertAll(
                () -> assertThat(thrown).as("thrown by the resume").isNull(),
                () -> assertThat(storage.refusedSaves).as("saves the storage refused").hasValue(0),
                () -> assertThat(storage.read(ID).asString()).as("stored position").isEqualTo(resumedAt.asString())
        );
    }

    // A takeover replay from the beginning whose live start the live model loses at each open, held once the catch-up
    // made 4 replays, with the catch-up position it stored. The live model keeps every open after this returns
    private void heldTakeoverReplayWithACatchupPositionStored() {
        LiveStartLosingSubscriptionModel liveModel = new LiveStartLosingSubscriptionModel(inMemorySubscriptionModel, true, false, () -> {
        }).losingEveryOpen();
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage, id -> OptionalLong.of(VERSION)).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        catchup = new StreamCatchupSubscriptionModel(liveModel, eventStore, config);
        catchup.addStartRefusalListener(new StartRefusalListener() {
            @Override
            public void handleStartRefusal(String subscriptionId, UnsupportedStartAtException refusal) {
            }

            @Override
            public void handleHeldPaused(String subscriptionId, UnsupportedStartAtException refusal) {
            }

            @Override
            public void handleHeldPaused(String subscriptionId, CatchupCannotResumeException refusal) {
                heldPaused.add(refusal);
            }

            @Override
            public void handleStoredPositionMovedEarlier(String subscriptionId, CheckpointWriteConditionNotFulfilledException refusal) {
            }
        });
        catchup.subscribe(ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> received.add(cloudEventConverter.toDomainEvent(cloudEvent)));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(heldPaused).as("held paused reports").hasSize(1));
        assertThat(storage.read(ID)).as("position the takeover replay stored").isNotNull().matches(GlobalCheckpoint::isGlobalCheckpoint, "a catch-up position");
        assertThat(storage.writeVersion(ID)).as("version stored with it").hasValue(VERSION);
        liveModel.keepingEveryOpen();
    }

    private static NameDefined nameDefined(LocalDateTime time, String name) {
        return new NameDefined(UUID.randomUUID().toString(), time, "name", name);
    }

    private void write(DomainEvent event) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(event));
        eventStore.write(event.eventId(), cloudEvents);
    }

    /**
     * Delegates to a real {@link InMemoryCheckpointStorage}, which evaluates write conditions, and stands in for the
     * MongoDB storages' refusal. After {@link #anotherStartMovedTheStoredPositionEarlier()} a {@code notOlderThan}
     * save is refused with the stored version, until a write goes through, or a resolution with a version or a
     * {@code storeWriteVersion} call keeps the stored checkpoint.
     */
    private static final class StorageThatRefusesOnceAStartMovedItEarlier implements CheckpointStorage {
        private final InMemoryCheckpointStorage delegate = new InMemoryCheckpointStorage();
        final AtomicInteger refusedSaves = new AtomicInteger();
        volatile boolean movedEarlier;

        void anotherStartMovedTheStoredPositionEarlier() {
            movedEarlier = true;
        }

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            return delegate.read(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            if (movedEarlier && condition instanceof CheckpointWriteCondition.NotOlderThan) {
                refusedSaves.incrementAndGet();
                throw new CheckpointWriteConditionNotFulfilledException(subscriptionId, delegate.writeVersion(subscriptionId), condition);
            }
            Checkpoint saved = delegate.save(subscriptionId, checkpoint, condition);
            movedEarlier = false;
            return saved;
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return delegate.evaluatesWriteConditions();
        }

        @Override
        public OptionalLong writeVersion(String subscriptionId) {
            return delegate.writeVersion(subscriptionId);
        }

        @Override
        public void delete(String subscriptionId) {
            delegate.delete(subscriptionId);
        }

        @Override
        public boolean exists(String subscriptionId) {
            return delegate.exists(subscriptionId);
        }

        @Override
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate) {
            return delegate.resolveFirstCheckpointRace(subscriptionId, candidate);
        }

        @Override
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate, long writeVersion) {
            Optional<Checkpoint> resolution = delegate.resolveFirstCheckpointRace(subscriptionId, candidate, writeVersion);
            movedEarlier = false;
            return resolution;
        }

        @Override
        public Optional<Checkpoint> storeWriteVersion(String subscriptionId, long writeVersion) {
            Optional<Checkpoint> stored = delegate.storeWriteVersion(subscriptionId, writeVersion);
            if (stored.isPresent()) {
                movedEarlier = false;
            }
            return stored;
        }
    }
}
