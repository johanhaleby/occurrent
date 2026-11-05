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

package org.occurrent.springboot.reactor;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartPosition;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.StartAt;
import org.springframework.context.ApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * The start that {@code startAt = NOW} produces for the capability-agnostic and the DCB annotations
 * ({@code @Subscription}, {@code @Projection}, {@code @Snapshot} and {@code @DcbSubscription}) depends on the
 * {@code resumeBehavior}, the same as the one {@code @StreamSubscription} produces. Under {@code DEFAULT} NOW is where
 * the subscription starts the first time, and a restart continues from the stored position, which is what the
 * subscription model default does. Under {@code SAME_AS_START_AT} every start is NOW. Either way the start is not fixed
 * for {@link LateSubscriber}, since it depends on when the subscribe runs.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class NowStartDependingOnResumeBehaviorTest {

    private static final String SUBSCRIPTION_ID = "now-start-depending-on-resume-behavior";

    private final StartPositionSupport support = new StartPositionSupport(mock(ApplicationContext.class));

    @ParameterizedTest
    @EnumSource(ResumeBehavior.class)
    void an_agnostic_now_start_is_not_fixed_whatever_the_resume_behavior(ResumeBehavior resumeBehavior) {
        // Given
        StartAt startAt = support.generateAgnosticStartAt(SUBSCRIPTION_ID, StartPosition.NOW, -1, resumeBehavior);

        // When
        boolean fixed = LateSubscriber.startIsFixed(startAt);

        // Then
        assertThat(fixed).describedAs("agnostic startAt = NOW with %s", resumeBehavior).isFalse();
    }

    @ParameterizedTest
    @EnumSource(ResumeBehavior.class)
    void a_dcb_now_start_is_not_fixed_whatever_the_resume_behavior(ResumeBehavior resumeBehavior) {
        // Given
        DcbStartAt startAt = support.generateDcbStartAt(SUBSCRIPTION_ID, StartPosition.NOW, -1, resumeBehavior);

        // When
        boolean fixed = LateSubscriber.startIsFixed(startAt);

        // Then
        assertThat(fixed).describedAs("DCB startAt = NOW with %s", resumeBehavior).isFalse();
    }

    @Test
    void an_agnostic_now_start_with_default_resume_behavior_is_the_subscription_model_default() {
        // Given
        StartAt startAt = support.generateAgnosticStartAt(SUBSCRIPTION_ID, StartPosition.NOW, -1, ResumeBehavior.DEFAULT);

        // When
        boolean modelDefault = startAt.isDefault();

        // Then
        assertThat(modelDefault).describedAs("agnostic startAt = NOW with DEFAULT resolves to %s", startAt).isTrue();
    }

    @Test
    void an_agnostic_now_start_with_same_as_start_at_is_now() {
        // Given
        StartAt startAt = support.generateAgnosticStartAt(SUBSCRIPTION_ID, StartPosition.NOW, -1, ResumeBehavior.SAME_AS_START_AT);

        // When
        boolean now = startAt.isNow();

        // Then
        assertThat(now).describedAs("agnostic startAt = NOW with SAME_AS_START_AT resolves to %s", startAt).isTrue();
    }

    @Test
    void a_dcb_now_start_with_default_resume_behavior_is_the_subscription_model_default() {
        // Given
        DcbStartAt startAt = support.generateDcbStartAt(SUBSCRIPTION_ID, StartPosition.NOW, -1, ResumeBehavior.DEFAULT);

        // When
        boolean modelDefault = startAt.toStartAt().isDefault();

        // Then
        assertThat(modelDefault).describedAs("DCB startAt = NOW with DEFAULT resolves to %s", startAt).isTrue();
    }

    @Test
    void a_dcb_now_start_with_same_as_start_at_is_now() {
        // Given
        DcbStartAt startAt = support.generateDcbStartAt(SUBSCRIPTION_ID, StartPosition.NOW, -1, ResumeBehavior.SAME_AS_START_AT);

        // When
        boolean now = startAt.toStartAt().isNow();

        // Then
        assertThat(now).describedAs("DCB startAt = NOW with SAME_AS_START_AT resolves to %s", startAt).isTrue();
    }
}
