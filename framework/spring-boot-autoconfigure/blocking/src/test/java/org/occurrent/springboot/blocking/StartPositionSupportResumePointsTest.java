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

package org.occurrent.springboot.blocking;

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
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.StartAtCheckpoint;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.blocking.competingconsumers.CompetingConsumerSubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * What {@link StartPositionSupport} hands a subscription model when the application declares a
 * {@link SubscriptionResumePoints} bean. It holds for the agnostic {@code @Subscription}, the stream-only
 * {@code @StreamSubscription} and {@code @DcbSubscription}, each of which builds a start.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StartPositionSupportResumePointsTest {

    private static final String ID = "resumed";
    private static final String ANSWER = "position:5;origin:0;liveFrom:token";
    private static final Checkpoint ANSWERED_CHECKPOINT = new StringBasedCheckpoint(ANSWER);

    private enum Annotation {AGNOSTIC, STREAM, DCB}

    private final List<String> asked = new CopyOnWriteArrayList<>();

    @ParameterizedTest
    @EnumSource(Annotation.class)
    void without_a_bean_a_now_start_is_the_plain_now_start_and_nothing_is_asked(Annotation annotation) {
        // Given
        StartPositionSupport support = supportWithout();

        // When
        Object start = start(annotation, support, StartPosition.NOW, ResumeBehavior.DEFAULT);

        // Then
        assertThat(resolve(start, Object.class)).isNotNull().matches(StartAt::isNow);
        assertThat(asked).isEmpty();
    }

    @Test
    void without_a_bean_a_dcb_default_start_is_the_plain_subscription_model_default() {
        // Given
        StartPositionSupport support = supportWithout();

        // When
        Object start = start(Annotation.DCB, support, StartPosition.DEFAULT, ResumeBehavior.DEFAULT);

        // Then
        assertThat(start).isSameAs(DcbStartAt.subscriptionModelDefault());
    }

    @ParameterizedTest
    @EnumSource(Annotation.class)
    void with_a_bean_the_answer_replaces_a_now_start_for_every_subscription_model(Annotation annotation) {
        // Given
        StartPositionSupport support = supportAnswering(ANSWER);
        Object start = start(annotation, support, StartPosition.NOW, ResumeBehavior.DEFAULT);

        // When
        List<@Nullable StartAt> resolved = resolveForEveryModel(start);

        // Then
        assertThat(resolved).hasSize(4).allSatisfy(startAt -> assertThat(checkpointOf(startAt)).isEqualTo(ANSWERED_CHECKPOINT));
    }

    @ParameterizedTest
    @EnumSource(Annotation.class)
    void with_a_bean_the_answer_replaces_a_default_start_for_every_subscription_model_the_catch_up_included(Annotation annotation) {
        // Given
        StartPositionSupport support = supportAnswering(ANSWER);
        Object start = start(annotation, support, StartPosition.DEFAULT, ResumeBehavior.DEFAULT);

        // When
        List<@Nullable StartAt> resolved = resolveForEveryModel(start);

        // Then
        assertThat(resolved).hasSize(4).allSatisfy(startAt -> assertThat(checkpointOf(startAt)).isEqualTo(ANSWERED_CHECKPOINT));
    }

    @ParameterizedTest
    @EnumSource(Annotation.class)
    void with_a_bean_the_answer_replaces_a_beginning_start_that_resumes_the_default_way(Annotation annotation) {
        // Given
        StartPositionSupport support = supportAnswering(ANSWER);
        Object start = start(annotation, support, StartPosition.BEGINNING, ResumeBehavior.DEFAULT);

        // When
        List<@Nullable StartAt> resolved = resolveForEveryModel(start);

        // Then
        assertThat(resolved).hasSize(4).allSatisfy(startAt -> assertThat(checkpointOf(startAt)).isEqualTo(ANSWERED_CHECKPOINT));
    }

    @ParameterizedTest
    @EnumSource(Annotation.class)
    void with_a_bean_that_answers_null_a_now_start_resolves_to_now(Annotation annotation) {
        // Given
        StartPositionSupport support = supportAnswering(null);
        Object start = start(annotation, support, StartPosition.NOW, ResumeBehavior.DEFAULT);

        // When
        List<@Nullable StartAt> resolved = resolveForEveryModel(start);

        // Then
        assertThat(resolved).hasSize(4).allSatisfy(startAt -> assertThat(startAt).isNotNull().matches(StartAt::isNow));
        assertThat(asked).as("the bean is still asked, once per model").containsExactly(ID, ID, ID, ID);
    }

    @ParameterizedTest
    @EnumSource(Annotation.class)
    void with_a_bean_that_answers_null_a_beginning_start_resolves_to_a_replay_from_the_first_event(Annotation annotation) {
        // Given
        StartPositionSupport support = supportAnswering(null);
        Object start = start(annotation, support, StartPosition.BEGINNING, ResumeBehavior.DEFAULT);

        // When
        List<@Nullable StartAt> resolved = resolveForEveryModel(start);

        // Then
        assertThat(resolved).hasSize(4).allSatisfy(startAt -> assertThat(startAt).isNotNull().matches(startAt1 -> !startAt1.isNow() && !startAt1.isDefault()));
    }

    @ParameterizedTest
    @EnumSource(Annotation.class)
    void with_a_bean_that_answers_null_a_default_start_resolves_as_it_does_without_the_bean(Annotation annotation) {
        // Given
        StartPositionSupport withoutBean = supportWithout();
        StartPositionSupport withBean = supportAnswering(null);

        // When
        List<@Nullable StartAt> withoutResolved = resolveForEveryModel(start(annotation, withoutBean, StartPosition.DEFAULT, ResumeBehavior.DEFAULT));
        List<@Nullable StartAt> withResolved = resolveForEveryModel(start(annotation, withBean, StartPosition.DEFAULT, ResumeBehavior.DEFAULT));

        // Then
        assertThat(describe(withResolved)).isEqualTo(describe(withoutResolved));
    }

    @ParameterizedTest
    @EnumSource(value = Annotation.class, names = {"AGNOSTIC", "DCB"})
    void with_a_bean_a_same_as_start_at_replay_stays_off_for_the_competing_consumer_and_the_durable_model_without_asking(Annotation annotation) {
        // Given
        StartPositionSupport support = supportAnswering(ANSWER);
        Object start = start(annotation, support, StartPosition.BEGINNING, ResumeBehavior.SAME_AS_START_AT);

        // When
        @Nullable StartAt competingConsumer = resolve(start, CompetingConsumerSubscriptionModel.class);
        @Nullable StartAt durable = resolve(start, DurableSubscriptionModel.class);

        // Then
        assertThat(competingConsumer).isNull();
        assertThat(durable).isNull();
        assertThat(asked).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(value = Annotation.class, names = {"AGNOSTIC", "DCB"})
    void with_a_bean_a_same_as_start_at_replay_starts_the_catch_up_from_the_answer(Annotation annotation) {
        // Given
        StartPositionSupport support = supportAnswering(ANSWER);
        Object start = start(annotation, support, StartPosition.BEGINNING, ResumeBehavior.SAME_AS_START_AT);

        // When
        @Nullable StartAt catchup = resolve(start, CatchupSubscriptionModel.class);

        // Then
        assertThat(checkpointOf(catchup)).isEqualTo(ANSWERED_CHECKPOINT);
        assertThat(asked).containsExactly(ID);
    }

    @ParameterizedTest
    @EnumSource(value = Annotation.class, names = {"AGNOSTIC", "DCB"})
    void with_a_bean_that_answers_null_a_same_as_start_at_replay_starts_the_catch_up_from_the_first_event(Annotation annotation) {
        // Given
        StartPositionSupport support = supportAnswering(null);
        Object start = start(annotation, support, StartPosition.BEGINNING, ResumeBehavior.SAME_AS_START_AT);

        // When
        @Nullable StartAt catchup = resolve(start, CatchupSubscriptionModel.class);

        // Then
        assertThat(checkpointOf(catchup)).isEqualTo(GlobalCheckpoint.of(0));
    }

    @Test
    void with_a_bean_a_stream_beginning_of_time_start_with_a_same_as_start_at_replay_stays_off_for_the_competing_consumer_and_the_durable_model_without_asking() {
        // Given
        StartPositionSupport support = supportAnswering(ANSWER);
        Object start = start(Annotation.STREAM, support, StartPosition.BEGINNING, ResumeBehavior.SAME_AS_START_AT);

        // When
        @Nullable StartAt competingConsumer = resolve(start, CompetingConsumerSubscriptionModel.class);
        @Nullable StartAt durable = resolve(start, DurableSubscriptionModel.class);
        @Nullable StartAt catchup = resolve(start, CatchupSubscriptionModel.class);

        // Then
        assertThat(competingConsumer).isNull();
        assertThat(durable).isNull();
        assertThat(checkpointOf(catchup)).isEqualTo(ANSWERED_CHECKPOINT);
        assertThat(asked).containsExactly(ID);
    }

    private List<@Nullable StartAt> resolveForEveryModel(Object start) {
        return Arrays.asList(
                resolve(start, CompetingConsumerSubscriptionModel.class),
                resolve(start, CatchupSubscriptionModel.class),
                resolve(start, DurableSubscriptionModel.class),
                resolve(start, Object.class));
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

    private static List<String> describe(List<@Nullable StartAt> resolved) {
        return resolved.stream().map(startAt -> startAt == null ? "null" : startAt.isNow() ? "now" : startAt.isDefault() ? "default" : startAt.toString()).toList();
    }

    private static Object start(Annotation annotation, StartPositionSupport support, StartPosition startPosition, ResumeBehavior resumeBehavior) {
        return switch (annotation) {
            case AGNOSTIC -> support.generateAgnosticStartAt(ID, startPosition, -1, resumeBehavior);
            case DCB -> support.generateDcbStartAt(ID, startPosition, -1, resumeBehavior);
            case STREAM -> support.generateStartAt(ID, new StartPositionSupport.StartPositionToUse.StartAtStartPosition(switch (startPosition) {
                case NOW -> org.occurrent.annotation.StreamSubscription.StartPosition.NOW;
                case DEFAULT -> org.occurrent.annotation.StreamSubscription.StartPosition.DEFAULT;
                case BEGINNING -> org.occurrent.annotation.StreamSubscription.StartPosition.BEGINNING_OF_TIME;
            }), resumeBehavior);
        };
    }

    private StartPositionSupport supportWithout() {
        return supportAnswering(null, false);
    }

    private StartPositionSupport supportAnswering(@Nullable String answer) {
        return supportAnswering(answer, true);
    }

    @SuppressWarnings("unchecked")
    private StartPositionSupport supportAnswering(@Nullable String answer, boolean beanDeclared) {
        ApplicationContext applicationContext = mock(ApplicationContext.class);
        ObjectProvider<SubscriptionResumePoints> provider = mock(ObjectProvider.class);
        SubscriptionResumePoints resumePoints = subscriptionId -> {
            asked.add(subscriptionId);
            return answer;
        };
        when(provider.getIfAvailable()).thenReturn(beanDeclared ? resumePoints : null);
        when(applicationContext.getBeanProvider(SubscriptionResumePoints.class)).thenReturn(provider);
        CheckpointStorage checkpointStorage = mock(CheckpointStorage.class);
        when(checkpointStorage.exists(ID)).thenReturn(false);
        when(applicationContext.getBean(CheckpointStorage.class)).thenReturn(checkpointStorage);
        return new StartPositionSupport(applicationContext);
    }
}
