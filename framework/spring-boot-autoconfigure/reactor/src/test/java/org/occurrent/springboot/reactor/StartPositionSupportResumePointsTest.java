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

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartPosition;
import org.occurrent.springboot.common.SubscriptionResumePoints;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModel;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * What {@link StartPositionSupport#withResumePoint} hands a reactive subscription model when the application declares a
 * {@link SubscriptionResumePoints} bean, for the capability-agnostic and the DCB starts.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StartPositionSupportResumePointsTest {

    private static final String ID = "resumed";
    private static final String ANSWER = "position:5;origin:0;liveFrom:token";
    private static final Checkpoint ANSWERED_CHECKPOINT = new StringBasedCheckpoint(ANSWER);

    private enum Annotation {AGNOSTIC, DCB}

    private final List<String> asked = new CopyOnWriteArrayList<>();

    @ParameterizedTest
    @EnumSource(Annotation.class)
    void without_a_bean_a_now_start_is_returned_unchanged_and_nothing_is_asked(Annotation annotation) {
        // Given
        StartPositionSupport support = supportWithout();
        Object built = built(annotation, support, StartPosition.NOW, ResumeBehavior.DEFAULT);

        // When
        Object start = withResumePoint(annotation, support, built);

        // Then
        assertThat(start).isSameAs(built);
        assertThat(asked).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(Annotation.class)
    void without_a_bean_a_default_start_is_returned_unchanged(Annotation annotation) {
        // Given
        StartPositionSupport support = supportWithout();
        Object built = built(annotation, support, StartPosition.DEFAULT, ResumeBehavior.DEFAULT);

        // When
        Object start = withResumePoint(annotation, support, built);

        // Then
        assertThat(start).isSameAs(built);
    }

    @ParameterizedTest
    @EnumSource(Annotation.class)
    void with_a_bean_the_answer_replaces_a_now_start_for_every_subscription_model(Annotation annotation) {
        // Given
        StartPositionSupport support = supportAnswering(ANSWER);
        Object start = withResumePoint(annotation, support, built(annotation, support, StartPosition.NOW, ResumeBehavior.DEFAULT));

        // When
        List<@Nullable StartAt> resolved = resolveForEveryModel(start);

        // Then
        assertThat(resolved).hasSize(2).allSatisfy(startAt -> assertThat(checkpointOf(startAt)).isEqualTo(ANSWERED_CHECKPOINT));
    }

    @ParameterizedTest
    @EnumSource(Annotation.class)
    void with_a_bean_the_answer_replaces_a_default_start_for_every_subscription_model(Annotation annotation) {
        // Given
        StartPositionSupport support = supportAnswering(ANSWER);
        Object start = withResumePoint(annotation, support, built(annotation, support, StartPosition.DEFAULT, ResumeBehavior.DEFAULT));

        // When
        List<@Nullable StartAt> resolved = resolveForEveryModel(start);

        // Then
        assertThat(resolved).hasSize(2).allSatisfy(startAt -> assertThat(checkpointOf(startAt)).isEqualTo(ANSWERED_CHECKPOINT));
    }

    @ParameterizedTest
    @EnumSource(Annotation.class)
    void with_a_bean_the_answer_replaces_a_beginning_start_that_resumes_the_default_way(Annotation annotation) {
        // Given
        StartPositionSupport support = supportAnswering(ANSWER);
        Object start = withResumePoint(annotation, support, built(annotation, support, StartPosition.BEGINNING, ResumeBehavior.DEFAULT));

        // When
        List<@Nullable StartAt> resolved = resolveForEveryModel(start);

        // Then
        assertThat(resolved).hasSize(2).allSatisfy(startAt -> assertThat(checkpointOf(startAt)).isEqualTo(ANSWERED_CHECKPOINT));
    }

    @ParameterizedTest
    @EnumSource(Annotation.class)
    void with_a_bean_that_answers_null_a_now_start_resolves_to_now_and_the_bean_is_asked(Annotation annotation) {
        // Given
        StartPositionSupport support = supportAnswering(null);
        Object start = withResumePoint(annotation, support, built(annotation, support, StartPosition.NOW, ResumeBehavior.DEFAULT));

        // When
        List<@Nullable StartAt> resolved = resolveForEveryModel(start);

        // Then
        assertThat(resolved).hasSize(2).allSatisfy(startAt -> assertThat(startAt).isNotNull().matches(StartAt::isNow));
        assertThat(asked).containsExactly(ID, ID);
    }

    @ParameterizedTest
    @EnumSource(Annotation.class)
    void with_a_bean_that_answers_null_a_default_start_resolves_to_the_subscription_model_default(Annotation annotation) {
        // Given
        StartPositionSupport support = supportAnswering(null);
        Object start = withResumePoint(annotation, support, built(annotation, support, StartPosition.DEFAULT, ResumeBehavior.DEFAULT));

        // When
        List<@Nullable StartAt> resolved = resolveForEveryModel(start);

        // Then
        assertThat(resolved).hasSize(2).allSatisfy(startAt -> assertThat(startAt).isNotNull().matches(StartAt::isDefault));
    }

    @ParameterizedTest
    @EnumSource(Annotation.class)
    void with_a_bean_that_answers_null_a_beginning_start_resolves_to_a_replay_from_the_first_event(Annotation annotation) {
        // Given
        StartPositionSupport support = supportAnswering(null);
        Object start = withResumePoint(annotation, support, built(annotation, support, StartPosition.BEGINNING, ResumeBehavior.DEFAULT));

        // When
        List<@Nullable StartAt> resolved = resolveForEveryModel(start);

        // Then
        assertThat(resolved).hasSize(2).allSatisfy(startAt -> assertThat(startAt).isNotNull().matches(replay -> !replay.isNow() && !replay.isDefault()));
    }

    @ParameterizedTest
    @EnumSource(Annotation.class)
    void with_a_bean_a_same_as_start_at_replay_stays_off_for_the_durable_model_without_asking(Annotation annotation) {
        // Given
        StartPositionSupport support = supportAnswering(ANSWER);
        Object start = withResumePoint(annotation, support, built(annotation, support, StartPosition.BEGINNING, ResumeBehavior.SAME_AS_START_AT));

        // When
        @Nullable StartAt durable = resolve(start, ReactorDurableSubscriptionModel.class);

        // Then
        assertThat(durable).isNull();
        assertThat(asked).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(Annotation.class)
    void with_a_bean_a_same_as_start_at_replay_starts_the_layer_above_the_durable_model_from_the_answer(Annotation annotation) {
        // Given
        StartPositionSupport support = supportAnswering(ANSWER);
        Object start = withResumePoint(annotation, support, built(annotation, support, StartPosition.BEGINNING, ResumeBehavior.SAME_AS_START_AT));

        // When
        @Nullable StartAt other = resolve(start, Object.class);

        // Then
        assertThat(checkpointOf(other)).isEqualTo(ANSWERED_CHECKPOINT);
        assertThat(asked).containsExactly(ID);
    }

    @Test
    void the_dcb_answer_is_the_checkpoint_string_the_subscription_delivered() {
        // Given
        StartPositionSupport support = supportAnswering(ANSWER);

        // When
        DcbStartAt start = support.withResumePoint(ID, DcbStartAt.now());

        // Then
        assertThat(checkpointOf(resolve(start, Object.class))).isEqualTo(ANSWERED_CHECKPOINT);
    }

    private List<@Nullable StartAt> resolveForEveryModel(Object start) {
        return Arrays.asList(resolve(start, ReactorDurableSubscriptionModel.class), resolve(start, Object.class));
    }

    private static Checkpoint checkpointOf(@Nullable StartAt startAt) {
        assertThat(startAt).isInstanceOf(StartAt.StartAtCheckpoint.class);
        return ((StartAt.StartAtCheckpoint) startAt).checkpoint;
    }

    private static @Nullable StartAt resolve(Object start, Class<?> subscriptionModelType) {
        SubscriptionModelContext context = new SubscriptionModelContext(subscriptionModelType);
        return switch (start) {
            case StartAt startAt -> startAt.get(context);
            case DcbStartAt dcbStartAt -> dcbStartAt.toStartAt().get(context);
            default -> throw new IllegalArgumentException("Unexpected start " + start);
        };
    }

    private static Object built(Annotation annotation, StartPositionSupport support, StartPosition startPosition, ResumeBehavior resumeBehavior) {
        return annotation == Annotation.AGNOSTIC
                ? support.generateAgnosticStartAt(ID, startPosition, -1, resumeBehavior)
                : support.generateDcbStartAt(ID, startPosition, -1, resumeBehavior);
    }

    private static Object withResumePoint(Annotation annotation, StartPositionSupport support, Object built) {
        return annotation == Annotation.AGNOSTIC
                ? support.withResumePoint(ID, (StartAt) built)
                : support.withResumePoint(ID, (DcbStartAt) built);
    }

    private StartPositionSupport supportWithout() {
        return support(null, false);
    }

    private StartPositionSupport supportAnswering(@Nullable String answer) {
        return support(answer, true);
    }

    @SuppressWarnings("unchecked")
    private StartPositionSupport support(@Nullable String answer, boolean beanDeclared) {
        ApplicationContext applicationContext = mock(ApplicationContext.class);
        ObjectProvider<SubscriptionResumePoints> provider = mock(ObjectProvider.class);
        SubscriptionResumePoints resumePoints = subscriptionId -> {
            asked.add(subscriptionId);
            return answer;
        };
        when(provider.getIfAvailable()).thenReturn(beanDeclared ? resumePoints : null);
        when(applicationContext.getBeanProvider(SubscriptionResumePoints.class)).thenReturn(provider);
        CheckpointStorage checkpointStorage = mock(CheckpointStorage.class);
        when(checkpointStorage.read(ID)).thenReturn(Mono.empty());
        when(applicationContext.getBean(CheckpointStorage.class)).thenReturn(checkpointStorage);
        return new StartPositionSupport(applicationContext);
    }
}
