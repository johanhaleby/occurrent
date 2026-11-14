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

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.api.blocking.internal.CatchupReadScope;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.internal.DcbCatchupReadScope;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.DcbSubscriptionFilter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.StartAtCheckpoint;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.subscription.internal.CatchupCheckpoints;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A catch-up model wired with both a stream store and a DCB store resumes a subscription at now from the present
 * position of the store that subscription was made on, since each store reads its own live start.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CatchupSubscriptionModelResumeAtNowOwnerTest {

    private static final String ID = "subscription";
    private static final Checkpoint TOKEN = new StringBasedCheckpoint("token-1");
    private static final StringBasedCheckpoint STREAM_LIVE_START = new StringBasedCheckpoint("stream-store-live-start");
    private static final StringBasedCheckpoint DCB_LIVE_START = new StringBasedCheckpoint("dcb-store-live-start");

    private InMemorySubscriptionModel inMemory;
    private InMemoryCheckpointStorage storage;
    private HistoryLosingLiveModel liveModel;
    private CatchupSubscriptionModel catchup;

    @BeforeEach
    void a_catchup_model_over_a_stream_store_and_a_dcb_store_with_different_live_starts() {
        inMemory = new InMemorySubscriptionModel();
        storage = new InMemoryCheckpointStorage();
        liveModel = new HistoryLosingLiveModel();
        InMemoryEventStore streamStore = new InMemoryEventStore(inMemory) {
            @Override
            public CatchupReadScope openCatchupReadScope(@Nullable String notBefore) {
                return new ForwardingCatchupReadScope(super.openCatchupReadScope(notBefore)) {
                    @Override
                    public @Nullable String liveStart() {
                        return STREAM_LIVE_START.asString();
                    }
                };
            }
        };
        InMemoryEventStore dcbStore = new InMemoryEventStore(inMemory) {
            @Override
            public DcbCatchupReadScope openDcbCatchupReadScope(@Nullable String notBefore) {
                DcbCatchupReadScope scope = super.openDcbCatchupReadScope(notBefore);
                return new DcbCatchupReadScope() {
                    @Override
                    public @Nullable String liveStart() {
                        return DCB_LIVE_START.asString();
                    }

                    @Override
                    public DcbEventStream read(DcbCriteria criteria, DcbReadOptions options) {
                        return scope.read(criteria, options);
                    }

                    @Override
                    public void close() {
                        scope.close();
                    }
                };
            }
        };
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        catchup = new CatchupSubscriptionModel(liveModel, streamStore, dcbStore, DcbCriteria.tags(Tag.parse("name:1")), config);
    }

    @AfterEach
    void shutdown() {
        catchup.shutdown();
        inMemory.shutdown();
    }

    @Test
    void a_resume_at_now_of_a_dcb_subscription_over_a_stored_catch_up_position_stores_the_live_start_of_the_dcb_store_and_resumes_the_live_model_at_it() {
        // Given
        catchup.subscribe(ID, DcbSubscriptionFilter.filter(DcbCriteria.tags(Tag.parse("name:1"))), StartAt.checkpoint(TOKEN), __ -> {
        });
        catchup.pauseSubscription(ID);
        storage.save(ID, new StringBasedCheckpoint(CatchupCheckpoints.format(2, 1, 3, TOKEN)));

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.now()));

        // Then
        assertThat(thrown).as("thrown by the resume at now").isNull();
        assertAll(
                () -> assertThat(storage.read(ID)).as("stored position, the live start of the DCB store").isEqualTo(DCB_LIVE_START),
                () -> assertThat(liveModel.resumedAt).as("starts the live model was resumed at").singleElement().isInstanceOfSatisfying(StartAtCheckpoint.class,
                        startAt -> assertThat(startAt.checkpoint).as("position the live model was resumed at").isEqualTo(DCB_LIVE_START))
        );
    }
}
