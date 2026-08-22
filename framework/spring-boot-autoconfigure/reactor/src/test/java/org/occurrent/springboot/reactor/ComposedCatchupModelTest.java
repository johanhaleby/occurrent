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
import org.occurrent.subscription.api.reactor.SubscriptionModelCapability;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.DelegatingIntroductionInterceptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

/**
 * {@code identifiedAs} runs inside the {@code @Bean} method with the raw target, but a later {@code getBean} lookup
 * (what {@code AsynchronousSubscribables.resolve} and a bare {@code getBean(FluxSubscriptionModel.class)} both are)
 * can return an AOP proxy around it, for example when the application adds advice to the durable model bean. A
 * fixed-singleton proxy has to compare equal to the target it wraps, or the shipped composition silently stops
 * warning the moment it is proxied.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ComposedCatchupModelTest {

    interface TestModel extends SubscriptionModelCapability {
    }

    interface Unrelated {
    }

    @Test
    void isDefaultKnownLiveOnlyFor_matches_a_fixed_singleton_proxy_of_the_identified_model() {
        TestModel target = mock(TestModel.class);
        TestModel proxy = (TestModel) new ProxyFactory(target).getProxy();

        ComposedCatchupModel holder = new ComposedCatchupModel();
        holder.identifiedAs(target);
        holder.defaultBypassesCatchup();

        assertThat(holder.isDefaultKnownLiveOnlyFor(proxy)).isTrue();
    }

    @Test
    void isDefaultKnownLiveOnlyFor_does_not_match_a_proxy_of_a_different_model() {
        TestModel target = mock(TestModel.class);
        TestModel otherModel = mock(TestModel.class);
        TestModel proxyOfOther = (TestModel) new ProxyFactory(otherModel).getProxy();

        ComposedCatchupModel holder = new ComposedCatchupModel();
        holder.identifiedAs(target);
        holder.defaultBypassesCatchup();

        assertThat(holder.isDefaultKnownLiveOnlyFor(proxyOfOther)).isFalse();
    }

    @Test
    void isDefaultKnownLiveOnlyFor_does_not_throw_when_a_proxys_target_does_not_implement_the_capability() {
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

        ComposedCatchupModel holder = new ComposedCatchupModel();
        assertThatCode(() -> holder.identifiedAs(proxy)).doesNotThrowAnyException();
        holder.defaultBypassesCatchup();

        assertThat(holder.isDefaultKnownLiveOnlyFor(proxy)).isTrue();
    }
}
