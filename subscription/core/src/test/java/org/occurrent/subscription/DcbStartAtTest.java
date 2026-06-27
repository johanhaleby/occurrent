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

package org.occurrent.subscription;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbStartAtTest {

    @Test
    void now_maps_to_StartAt_now() {
        StartAt startAt = DcbStartAt.now().toStartAt();
        assertThat(startAt.isNow()).isTrue();
    }

    @Test
    void subscription_model_default_maps_to_StartAt_default() {
        StartAt startAt = DcbStartAt.subscriptionModelDefault().toStartAt();
        assertThat(startAt.isDefault()).isTrue();
    }

    @Test
    void beginning_maps_to_dcb_position_zero() {
        StartAt startAt = DcbStartAt.beginning().toStartAt();
        assertThat(startAt).isInstanceOf(StartAt.StartAtSubscriptionPosition.class);
        assertThat(((StartAt.StartAtSubscriptionPosition) startAt).subscriptionPosition).isEqualTo(DcbSubscriptionPosition.of(0));
    }

    @Test
    void after_position_maps_to_the_given_dcb_position() {
        StartAt startAt = DcbStartAt.afterPosition(5).toStartAt();
        assertThat(startAt).isInstanceOf(StartAt.StartAtSubscriptionPosition.class);
        assertThat(((StartAt.StartAtSubscriptionPosition) startAt).subscriptionPosition).isEqualTo(DcbSubscriptionPosition.of(5));
    }

    @Test
    void after_position_rejects_a_negative_position_eagerly() {
        assertThatThrownBy(() -> DcbStartAt.afterPosition(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("negative");
    }

    @Test
    void dynamic_returning_null_propagates_null_to_the_caller() {
        // A read-model delegate that always returns null signals "no opinion" to the subscription model.
        StartAt.SubscriptionModelContext ctx = new StartAt.SubscriptionModelContext(Object.class);

        StartAt resolved = DcbStartAt.dynamic(ignored -> null).toStartAt().get(ctx);

        assertThat(resolved).isNull();
    }

    @Test
    void dynamic_returning_beginning_resolves_to_dcb_position_zero() {
        StartAt.SubscriptionModelContext ctx = new StartAt.SubscriptionModelContext(Object.class);

        StartAt resolved = DcbStartAt.dynamic(ignored -> DcbStartAt.beginning()).toStartAt().get(ctx);

        assertThat(resolved).isInstanceOf(StartAt.StartAtSubscriptionPosition.class);
        assertThat(((StartAt.StartAtSubscriptionPosition) resolved).subscriptionPosition)
                .isEqualTo(DcbSubscriptionPosition.of(0));
    }
}
