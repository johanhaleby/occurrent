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

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.api.blocking.SubscriptionModelCapability;
import org.springframework.aop.framework.ProxyFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * {@code suppliedBy} runs inside the {@code @Bean} method with the raw target, but a later {@code getBean} lookup
 * (what a projection's own capability lookup ultimately is) can return an AOP proxy around it, for example when the
 * application adds advice to the composed model bean. A fixed-singleton proxy has to compare equal to the target it
 * wraps, or the shipped composition silently stops warning the moment it is proxied.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ComposedDefaultStartPositionTest {

    interface TestModel extends SubscriptionModelCapability {
    }

    @Test
    void isDefaultKnownLiveOnlyFor_matches_a_fixed_singleton_proxy_of_the_supplied_model() {
        TestModel target = mock(TestModel.class);
        TestModel proxy = (TestModel) new ProxyFactory(target).getProxy();

        ComposedDefaultStartPosition holder = new ComposedDefaultStartPosition();
        holder.suppliedBy(target);
        holder.defaultBypassesCatchup();

        assertThat(holder.isDefaultKnownLiveOnlyFor(proxy)).isTrue();
    }

    @Test
    void isDefaultKnownLiveOnlyFor_does_not_match_a_proxy_of_a_different_model() {
        TestModel target = mock(TestModel.class);
        TestModel otherModel = mock(TestModel.class);
        TestModel proxyOfOther = (TestModel) new ProxyFactory(otherModel).getProxy();

        ComposedDefaultStartPosition holder = new ComposedDefaultStartPosition();
        holder.suppliedBy(target);
        holder.defaultBypassesCatchup();

        assertThat(holder.isDefaultKnownLiveOnlyFor(proxyOfOther)).isFalse();
    }

    @Test
    void isDefaultKnownLiveOnlyFor_is_false_for_null_even_when_defaultBypassesCatchup_was_recorded() {
        ComposedDefaultStartPosition holder = new ComposedDefaultStartPosition();
        holder.suppliedBy(mock(TestModel.class));
        holder.defaultBypassesCatchup();

        assertThat(holder.isDefaultKnownLiveOnlyFor(null)).isFalse();
    }

    @Test
    void isDefaultKnownLiveOnlyFor_is_false_when_defaultBypassesCatchup_was_never_called() {
        TestModel target = mock(TestModel.class);
        ComposedDefaultStartPosition holder = new ComposedDefaultStartPosition();
        holder.suppliedBy(target);

        assertThat(holder.isDefaultKnownLiveOnlyFor(target)).isFalse();
    }

    @Test
    void suppliedBy_refuses_a_second_call() {
        ComposedDefaultStartPosition holder = new ComposedDefaultStartPosition();
        holder.suppliedBy(mock(TestModel.class));

        assertThatThrownBy(() -> holder.suppliedBy(mock(TestModel.class)))
                .isInstanceOf(IllegalStateException.class);
    }
}
