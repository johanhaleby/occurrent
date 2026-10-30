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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.competingconsumers.CompetingConsumerSubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * What a {@code NOW} start answers per subscription model layer. Under {@link ResumeBehavior#SAME_AS_START_AT} every
 * start is {@code NOW}, so the durable and competing consumer layers answer {@code null} to delegate to their parent
 * and keep no position, while the layers beneath them start now. Under {@link ResumeBehavior#DEFAULT} the start is the
 * plain now start, which leaves a restart to the durable layer.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class NowStartResumeBehaviorStartAtTest {

    private static final String ID = "now-subscription";
    private static final long NO_POSITION = -1;

    static class DurableSubscriptionModelSubclass extends DurableSubscriptionModel {
        DurableSubscriptionModelSubclass() {
            super(mock(CheckpointAwareSubscriptionModel.class), mock(CheckpointStorage.class));
        }
    }

    static class CompetingConsumerSubscriptionModelSubclass extends CompetingConsumerSubscriptionModel {
        CompetingConsumerSubscriptionModelSubclass() {
            super(mock(SubscriptionModel.class), mock(CompetingConsumerStrategy.class));
        }
    }

    static Stream<Class<?>> layersThatKeepThePosition() {
        return Stream.of(
                DurableSubscriptionModel.class,
                DurableSubscriptionModelSubclass.class,
                CompetingConsumerSubscriptionModel.class,
                CompetingConsumerSubscriptionModelSubclass.class);
    }

    static Stream<Class<?>> layersThatStartNow() {
        return Stream.of(CatchupSubscriptionModel.class, SubscriptionModel.class, Object.class);
    }

    static Stream<Class<?>> allLayers() {
        return Stream.concat(layersThatKeepThePosition(), layersThatStartNow());
    }

    @ParameterizedTest
    @MethodSource("layersThatKeepThePosition")
    void same_as_start_at_makes_a_now_start_answer_null_for_a_durable_or_competing_consumer_layer_and_its_subclasses(Class<?> subscriptionModelType) {
        // Given
        StartPositionSupport support = support();

        // When
        StartAt start = support.generateAgnosticStartAt(ID, StartPosition.NOW, NO_POSITION, ResumeBehavior.SAME_AS_START_AT);

        // Then
        assertThat(start.isDynamic()).isTrue();
        assertThat(start.get(contextOf(subscriptionModelType))).isNull();
    }

    @ParameterizedTest
    @MethodSource("layersThatStartNow")
    void same_as_start_at_makes_a_now_start_answer_now_for_any_other_layer(Class<?> subscriptionModelType) {
        // Given
        StartPositionSupport support = support();

        // When
        StartAt start = support.generateAgnosticStartAt(ID, StartPosition.NOW, NO_POSITION, ResumeBehavior.SAME_AS_START_AT);

        // Then
        @Nullable StartAt answer = start.get(contextOf(subscriptionModelType));
        assertThat(answer).isNotNull();
        assertThat(answer.isNow()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("allLayers")
    void default_resume_behavior_makes_a_now_start_the_plain_now_start_whatever_the_layer(Class<?> subscriptionModelType) {
        // Given
        StartPositionSupport support = support();

        // When
        StartAt start = support.generateAgnosticStartAt(ID, StartPosition.NOW, NO_POSITION, ResumeBehavior.DEFAULT);

        // Then
        assertThat(start.isNow()).isTrue();
        assertThat(start.isDynamic()).isFalse();
        @Nullable StartAt answer = start.get(contextOf(subscriptionModelType));
        assertThat(answer).isNotNull();
        assertThat(answer.isNow()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("layersThatKeepThePosition")
    void same_as_start_at_makes_a_dcb_now_start_answer_null_for_a_durable_or_competing_consumer_layer_and_its_subclasses(Class<?> subscriptionModelType) {
        // Given
        StartPositionSupport support = support();

        // When
        DcbStartAt start = support.generateDcbStartAt(ID, StartPosition.NOW, NO_POSITION, ResumeBehavior.SAME_AS_START_AT);

        // Then
        assertThat(start).isInstanceOf(DcbStartAt.Dynamic.class);
        assertThat(resolve(start, subscriptionModelType)).isNull();
    }

    @ParameterizedTest
    @MethodSource("layersThatStartNow")
    void same_as_start_at_makes_a_dcb_now_start_answer_now_for_any_other_layer(Class<?> subscriptionModelType) {
        // Given
        StartPositionSupport support = support();

        // When
        DcbStartAt start = support.generateDcbStartAt(ID, StartPosition.NOW, NO_POSITION, ResumeBehavior.SAME_AS_START_AT);

        // Then
        assertThat(resolve(start, subscriptionModelType)).isSameAs(DcbStartAt.now());
    }

    @ParameterizedTest
    @MethodSource("allLayers")
    void default_resume_behavior_makes_a_dcb_now_start_the_plain_now_start_whatever_the_layer(Class<?> subscriptionModelType) {
        // Given
        StartPositionSupport support = support();

        // When
        DcbStartAt start = support.generateDcbStartAt(ID, StartPosition.NOW, NO_POSITION, ResumeBehavior.DEFAULT);

        // Then
        assertThat(start).isSameAs(DcbStartAt.now());
        assertThat(resolve(start, subscriptionModelType)).isSameAs(DcbStartAt.now());
    }

    @ParameterizedTest
    @MethodSource("layersThatKeepThePosition")
    void same_as_start_at_makes_a_stream_subscription_now_start_answer_null_for_a_durable_or_competing_consumer_layer_and_its_subclasses(Class<?> subscriptionModelType) {
        // Given
        StartPositionSupport support = support();

        // When
        StartAt start = streamNowStart(support, ResumeBehavior.SAME_AS_START_AT);

        // Then
        assertThat(start.isDynamic()).isTrue();
        assertThat(start.get(contextOf(subscriptionModelType))).isNull();
    }

    @ParameterizedTest
    @MethodSource("layersThatStartNow")
    void same_as_start_at_makes_a_stream_subscription_now_start_answer_now_for_any_other_layer(Class<?> subscriptionModelType) {
        // Given
        StartPositionSupport support = support();

        // When
        StartAt start = streamNowStart(support, ResumeBehavior.SAME_AS_START_AT);

        // Then
        @Nullable StartAt answer = start.get(contextOf(subscriptionModelType));
        assertThat(answer).isNotNull();
        assertThat(answer.isNow()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("allLayers")
    void default_resume_behavior_makes_a_stream_subscription_now_start_the_plain_now_start_whatever_the_layer(Class<?> subscriptionModelType) {
        // Given
        StartPositionSupport support = support();

        // When
        StartAt start = streamNowStart(support, ResumeBehavior.DEFAULT);

        // Then
        assertThat(start.isNow()).isTrue();
        assertThat(start.isDynamic()).isFalse();
        @Nullable StartAt answer = start.get(contextOf(subscriptionModelType));
        assertThat(answer).isNotNull();
        assertThat(answer.isNow()).isTrue();
    }

    private static StartAt streamNowStart(StartPositionSupport support, ResumeBehavior resumeBehavior) {
        return support.generateStartAt(ID, new StartPositionSupport.StartPositionToUse.StartAtStartPosition(StreamSubscription.StartPosition.NOW), resumeBehavior);
    }

    private static SubscriptionModelContext contextOf(Class<?> subscriptionModelType) {
        return new SubscriptionModelContext(subscriptionModelType);
    }

    private static @Nullable DcbStartAt resolve(DcbStartAt start, Class<?> subscriptionModelType) {
        @Nullable DcbStartAt resolved = start;
        while (resolved instanceof DcbStartAt.Dynamic dynamic) {
            resolved = dynamic.function().apply(contextOf(subscriptionModelType));
        }
        return resolved;
    }

    @SuppressWarnings("unchecked")
    private static StartPositionSupport support() {
        ApplicationContext applicationContext = mock(ApplicationContext.class);
        ObjectProvider<org.occurrent.springboot.common.SubscriptionResumePoints> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(null);
        when(applicationContext.getBeanProvider(org.occurrent.springboot.common.SubscriptionResumePoints.class)).thenReturn(provider);
        return new StartPositionSupport(applicationContext);
    }
}
