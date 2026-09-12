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
import org.occurrent.subscription.api.reactor.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.reactor.SubscriptionModelCapability;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.DelegatingIntroductionInterceptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * The reactor twin of {@code ComposedDefaultStartPositionTest}, with the extra thing this holder owns, a catch-up
 * layer rather than the {@code DEFAULT} fact alone. Both answers are about one composition and are bound to that
 * composition's identity, so a projection running on some other model the context also holds gets neither
 * (<a href="https://github.com/johanhaleby/occurrent/issues/996">#996</a>).
 * <p>
 * {@code suppliedBy} runs inside the {@code @Bean} method with the raw target, but a later {@code getBean} lookup
 * (what a projection's own capability lookup ultimately is) can return an AOP proxy around it, for example when the
 * application adds advice to the composed model bean. A fixed-singleton proxy has to compare equal to the target it
 * wraps, or the shipped composition silently stops answering the moment it is proxied.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ComposedCatchupModelTest {

    interface TestModel extends SubscriptionModelCapability {
    }

    interface Unrelated {
    }

    @Test
    void catchupModelFor_matches_a_fixed_singleton_proxy_of_the_supplied_model() {
        TestModel target = mock(TestModel.class);
        TestModel proxy = (TestModel) new ProxyFactory(target).getProxy();
        ReplayAwareSubscriptions catchupLayer = mock(ReplayAwareSubscriptions.class);

        ComposedCatchupModel holder = new ComposedCatchupModel();
        holder.suppliedBy(target, catchupLayer);

        assertThat(holder.catchupModelFor(proxy)).contains(catchupLayer);
    }

    @Test
    void isDefaultKnownLiveOnlyFor_matches_a_fixed_singleton_proxy_of_the_supplied_model() {
        TestModel target = mock(TestModel.class);
        TestModel proxy = (TestModel) new ProxyFactory(target).getProxy();

        ComposedCatchupModel holder = new ComposedCatchupModel();
        holder.suppliedBy(target, mock(ReplayAwareSubscriptions.class));
        holder.defaultBypassesCatchup();

        assertThat(holder.isDefaultKnownLiveOnlyFor(proxy)).isTrue();
    }

    @Test
    void both_answers_match_the_raw_target_when_the_holder_was_supplied_a_proxy_of_it() {
        // The other way round from the tests above. Auto-proxying can happen before the bean method hands the model
        // over, so the proxy can end up on either side of the comparison and neither side may assume it is raw.
        TestModel target = mock(TestModel.class);
        TestModel proxy = (TestModel) new ProxyFactory(target).getProxy();
        ReplayAwareSubscriptions catchupLayer = mock(ReplayAwareSubscriptions.class);

        ComposedCatchupModel holder = new ComposedCatchupModel();
        holder.suppliedBy(proxy, catchupLayer);
        holder.defaultBypassesCatchup();

        assertThat(holder.isSuppliedFor(target)).isTrue();
        assertThat(holder.catchupModelFor(target)).contains(catchupLayer);
        assertThat(holder.isDefaultKnownLiveOnlyFor(target)).isTrue();
    }

    @Test
    void neither_answer_matches_a_different_model() {
        // The whole point of #996. A second bean of the same type in the context is not this composition, and being
        // told otherwise hands a whole replay to a layer that has never heard of that subscription id.
        TestModel target = mock(TestModel.class);
        TestModel otherModel = mock(TestModel.class);

        ComposedCatchupModel holder = new ComposedCatchupModel();
        holder.suppliedBy(target, mock(ReplayAwareSubscriptions.class));
        holder.defaultBypassesCatchup();

        assertThat(holder.isSuppliedFor(otherModel)).isFalse();
        assertThat(holder.catchupModelFor(otherModel)).isEmpty();
        assertThat(holder.isDefaultKnownLiveOnlyFor(otherModel)).isFalse();
    }

    @Test
    void neither_answer_matches_a_proxy_of_a_different_model() {
        TestModel target = mock(TestModel.class);
        TestModel otherModel = mock(TestModel.class);
        TestModel proxyOfOther = (TestModel) new ProxyFactory(otherModel).getProxy();

        ComposedCatchupModel holder = new ComposedCatchupModel();
        holder.suppliedBy(target, mock(ReplayAwareSubscriptions.class));
        holder.defaultBypassesCatchup();

        assertThat(holder.isSuppliedFor(proxyOfOther)).isFalse();
        assertThat(holder.catchupModelFor(proxyOfOther)).isEmpty();
        assertThat(holder.isDefaultKnownLiveOnlyFor(proxyOfOther)).isFalse();
    }

    @Test
    void neither_answer_matches_null_even_when_the_holder_was_supplied_and_defaultBypassesCatchup_was_recorded() {
        // null is what the registrar passes for a model exposing no capability at all, an identity there is nothing
        // to match against, so it must read as "not this composition" rather than as a wildcard.
        ComposedCatchupModel holder = new ComposedCatchupModel();
        holder.suppliedBy(mock(TestModel.class), mock(ReplayAwareSubscriptions.class));
        holder.defaultBypassesCatchup();

        assertThat(holder.isSuppliedFor(null)).isFalse();
        assertThat(holder.catchupModelFor(null)).isEmpty();
        assertThat(holder.isDefaultKnownLiveOnlyFor(null)).isFalse();
    }

    @Test
    void catchupModelFor_is_empty_for_the_very_model_it_was_supplied_for_when_that_composition_has_no_catchup_layer() {
        // Two different answers for the same candidate, and the pair is the assertion. Empty here is this
        // composition's own known fact (a store with no catch-up layer, ADR 132 decision 9), which isSuppliedFor
        // still reporting true is what separates it from the unknown the registrar has to warn about.
        TestModel target = mock(TestModel.class);
        Unrelated notReplayAware = mock(Unrelated.class);

        ComposedCatchupModel holder = new ComposedCatchupModel();
        holder.suppliedBy(target, notReplayAware);

        assertThat(holder.isSuppliedFor(target)).isTrue();
        assertThat(holder.catchupModelFor(target)).isEmpty();
    }

    @Test
    void isDefaultKnownLiveOnlyFor_is_false_when_defaultBypassesCatchup_was_never_called() {
        TestModel target = mock(TestModel.class);
        ComposedCatchupModel holder = new ComposedCatchupModel();
        holder.suppliedBy(target, mock(ReplayAwareSubscriptions.class));

        assertThat(holder.isDefaultKnownLiveOnlyFor(target)).isFalse();
    }

    @Test
    void the_answers_do_not_throw_when_a_proxys_target_does_not_implement_the_capability() {
        // A valid AOP shape: the proxy implements TestModel through an introduction, but its own target class does
        // not. Unwrapping past that target would try to cast a plain Object to TestModel, so ultimateTarget must
        // stop one layer short instead of throwing ClassCastException.
        Unrelated target = mock(Unrelated.class);
        TestModel delegate = mock(TestModel.class);
        ProxyFactory factory = new ProxyFactory(target);
        factory.setProxyTargetClass(false);
        factory.addAdvice(new DelegatingIntroductionInterceptor(delegate));
        factory.addInterface(TestModel.class);
        TestModel proxy = (TestModel) factory.getProxy();
        ReplayAwareSubscriptions catchupLayer = mock(ReplayAwareSubscriptions.class);

        ComposedCatchupModel holder = new ComposedCatchupModel();
        assertThatCode(() -> holder.suppliedBy(proxy, catchupLayer)).doesNotThrowAnyException();
        holder.defaultBypassesCatchup();

        assertThat(holder.catchupModelFor(proxy)).contains(catchupLayer);
        assertThat(holder.isDefaultKnownLiveOnlyFor(proxy)).isTrue();
    }

    @Test
    void suppliedBy_refuses_a_second_call() {
        ComposedCatchupModel holder = new ComposedCatchupModel();
        holder.suppliedBy(mock(TestModel.class), mock(ReplayAwareSubscriptions.class));

        assertThatThrownBy(() -> holder.suppliedBy(mock(TestModel.class), mock(ReplayAwareSubscriptions.class)))
                .isInstanceOf(IllegalStateException.class);
    }
}
