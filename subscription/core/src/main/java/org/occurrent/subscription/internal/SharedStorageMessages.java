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
 * The sentence a message appends when it tells someone to cancel a subscription, or to delete or replace what is
 * stored under its id. The blocking and reactor messages use the same constants, so they say the same thing.
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
     * For a message that tells someone to cancel a catch-up-then-live subscription, or to delete what is stored under its
     * id, such as its catch-up marker.
     */
    public static final String MARKER_WHEN_SEVERAL_INSTANCES_RUN_IT = "When more than one instance runs it, stop every instance,"
            + " delete what is stored under its id, and start every instance again with the same start, as " + GUIDE + " describes.";
}
