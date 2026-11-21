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

package org.occurrent.subscription.internal;

/**
 * The sentence a message appends when it tells someone to cancel a subscription, to delete or replace its checkpoint
 * or catch-up marker, or to subscribe under a new id. The blocking and reactor messages use the same constants, so
 * they say the same thing.
 * <p>
 * Every instance of an application reads the same checkpoint or catch-up marker from shared storage. Under a
 * {@code CompetingConsumerSubscriptionModel} the instance that gets the lease next writes the checkpoint, and on the
 * reactor stack and for a push subscription every instance runs the subscription and writes it. So a cancel on one
 * instance, or a delete while another one runs, lets that other instance store a newer checkpoint or a marker before
 * the instance being recovered subscribes again.
 */
public final class SharedStorageMessages {

    private SharedStorageMessages() {
    }

    /**
     * The upgrade guide section that says how to change a checkpoint or a catch-up marker that several instances share.
     */
    public static final String GUIDE = "https://github.com/johanhaleby/occurrent/blob/main/doc/migration/upgrading-to-0.35.0.md#changing-a-checkpoint-or-marker-that-several-instances-share";

    /**
     * For a message that tells someone to cancel a subscription that stores a checkpoint, or to delete or replace that
     * checkpoint.
     */
    public static final String CHECKPOINT_WHEN_SEVERAL_INSTANCES_RUN_IT = "When more than one instance runs the subscription, stop every instance,"
            + " delete its stored checkpoint or replace it with the checkpoint to start from, and start every instance again with the same start, as " + GUIDE + " describes.";

    /**
     * For a message that tells someone to cancel a catch-up-then-live subscription, or to delete its catch-up marker. It
     * names the checkpoint storage that keeps the marker, since a push saga keeps its instances under the same id in its
     * saga state store.
     */
    public static final String MARKER_WHEN_SEVERAL_INSTANCES_RUN_IT = "When more than one instance runs it, stop every instance,"
            + " delete the catch-up marker the checkpoint storage keeps under its id, and start every instance again with the same start, as " + GUIDE + " describes.";

    /**
     * For a message about a checkpoint stored under the id that is not a catch-up marker, which something else, such as
     * a durable subscription, may still use. The sentence before it names that checkpoint and says to delete or cancel
     * only when nothing else uses it, and this keeps the same condition, so it never asks for an unconditional delete of
     * another component's checkpoint.
     */
    public static final String NOT_A_MARKER_WHEN_SEVERAL_INSTANCES_RUN_IT = "When nothing else uses that checkpoint and more than one instance"
            + " runs the subscription, stop every instance, delete the checkpoint, and start every instance again with the same start, as " + GUIDE + " describes.";

    /**
     * For a message that tells someone to subscribe under a new id, or to replace a catch-up-then-live subscription
     * with one under a new id. A new id keeps the new subscription apart from what is stored under the old id, but not
     * from the broker queue or consumer group the old one consumes, nor from a store the old one still writes to.
     */
    public static final String NEW_ID_WHEN_SEVERAL_INSTANCES_RUN_IT = "Under a new id the subscription starts from the beginning of the history"
            + " and repeats everything its handler does, the records it writes, the commands it dispatches and the messages it sends."
            + " Stop every instance on the old id before the first one on the new id starts. Stopping them doesn't prevent the repeats."
            + " With more than one instance, what else a new id needs depends on where each instance keeps what it builds and which"
            + " broker queue or consumer group it consumes, as " + GUIDE + " describes.";
}
