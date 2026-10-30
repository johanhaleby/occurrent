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
import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.StreamSubscription.StartPosition;
import org.occurrent.springboot.common.SubscriptionAnnotations.StreamSubscriptionDefinition;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.springframework.context.ApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * A lazy {@code @StreamSubscription} bean built on a Reactor non-blocking thread is subscribed on another thread when
 * {@link LateSubscriber#startIsFixed(StartAt)} says its start is the same whenever it runs. A start that depends on
 * when the subscribe runs, NOW or the subscription model default, has to be subscribed where it is, or whatever is
 * written between getting the bean and the subscribe is skipped. So the start that
 * {@code startAt = NOW} produces must never be classified as fixed, whatever the {@code resumeBehavior}. A replay
 * start, {@code startAt = BEGINNING_OF_TIME} on a model that supports history replay, is the same whenever it runs, so
 * it is fixed whatever the {@code resumeBehavior}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamSubscriptionStartDependingOnWhenItSubscribesTest {

    private final StartPositionSupport support = new StartPositionSupport(mock(ApplicationContext.class));

    @ParameterizedTest
    @EnumSource(ResumeBehavior.class)
    void a_now_start_is_not_fixed_whatever_the_resume_behavior(ResumeBehavior resumeBehavior) {
        // Given
        StartAt startAt = support.generateStreamStartAt(definition(StartPosition.NOW, resumeBehavior), true);

        // When
        boolean fixed = LateSubscriber.startIsFixed(startAt);

        // Then
        assertThat(fixed).describedAs("startAt = NOW with %s", resumeBehavior).isFalse();
    }

    @ParameterizedTest
    @EnumSource(ResumeBehavior.class)
    void a_default_start_is_not_fixed_whatever_the_resume_behavior(ResumeBehavior resumeBehavior) {
        // Given
        StartAt startAt = support.generateStreamStartAt(definition(StartPosition.DEFAULT, resumeBehavior), true);

        // When
        boolean fixed = LateSubscriber.startIsFixed(startAt);

        // Then
        assertThat(fixed).describedAs("startAt = DEFAULT with %s", resumeBehavior).isFalse();
    }

    @ParameterizedTest
    @EnumSource(ResumeBehavior.class)
    void a_beginning_of_time_start_is_fixed_when_history_replay_is_supported(ResumeBehavior resumeBehavior) {
        // Given
        StartAt startAt = support.generateStreamStartAt(definition(StartPosition.BEGINNING_OF_TIME, resumeBehavior), true);

        // When
        boolean fixed = LateSubscriber.startIsFixed(startAt);

        // Then
        assertThat(fixed).describedAs("startAt = BEGINNING_OF_TIME with %s", resumeBehavior).isTrue();
    }

    // The inverse of the ones above, so that the classification is not green because it never answers true.
    @Test
    void a_start_at_an_explicit_position_is_fixed() {
        // Given
        StartAt startAt = StartAt.checkpoint(GlobalCheckpoint.of(0));

        // When
        boolean fixed = LateSubscriber.startIsFixed(startAt);

        // Then
        assertThat(fixed).isTrue();
    }

    private static StreamSubscriptionDefinition definition(StartPosition startAt, ResumeBehavior resumeBehavior) {
        return new StreamSubscriptionDefinition("start-depending-on-when-it-subscribes", new Class<?>[0], "", -1,
                startAt, resumeBehavior, StartupMode.DEFAULT, "@StreamSubscription");
    }
}
