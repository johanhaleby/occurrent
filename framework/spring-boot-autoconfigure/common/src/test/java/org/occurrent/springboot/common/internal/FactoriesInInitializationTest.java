/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.springboot.common.internal;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.FactoryBean;

import java.lang.ref.WeakReference;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class FactoriesInInitializationTest {

    private static final long GC_DEADLINE_NANOS = 10_000_000_000L;

    private final FactoriesInInitialization factories = new FactoriesInInitialization();

    @Test
    void a_factory_that_started_is_recognised_once_at_its_end_and_not_again() {
        StubFactory factory = new StubFactory();
        factories.initializationStarts(factory);

        assertThat(factories.initializationEnds(factory)).describedAs("first end").isTrue();
        assertThat(factories.initializationEnds(factory)).describedAs("second end").isFalse();
    }

    @Test
    void a_product_that_implements_FactoryBean_and_never_started_is_not_recognised_while_its_factory_is() {
        StubFactory factory = new StubFactory();
        StubFactory product = new StubFactory();
        factories.initializationStarts(factory);

        assertThat(factories.initializationEnds(product)).describedAs("the product").isFalse();
        assertThat(factories.initializationEnds(factory)).describedAs("the factory").isTrue();
    }

    @Test
    void a_factory_that_started_twice_is_recognised_at_each_of_its_two_ends_and_not_at_a_third() {
        StubFactory factory = new StubFactory();
        factories.initializationStarts(factory);
        factories.initializationStarts(factory);

        assertThat(factories.initializationEnds(factory)).describedAs("inner end").isTrue();
        assertThat(factories.initializationEnds(factory)).describedAs("outer end").isTrue();
        assertThat(factories.initializationEnds(factory)).describedAs("third end").isFalse();
    }

    @Test
    void something_that_never_started_is_not_recognised_when_no_factory_started_on_the_thread() {
        assertThat(factories.initializationEnds(new StubFactory())).isFalse();
    }

    @Test
    void a_proxy_whose_ultimate_target_is_the_factory_is_recognised() {
        StubFactory factory = new StubFactory();
        ProxyFactory proxyFactory = new ProxyFactory(factory);
        proxyFactory.setProxyTargetClass(false);
        proxyFactory.setInterfaces(FactoryBean.class);
        Object proxy = proxyFactory.getProxy();
        assertThat(proxy).isNotSameAs(factory);
        factories.initializationStarts(factory);

        assertThat(factories.initializationEnds(proxy)).isTrue();
    }

    @Test
    void a_factory_that_started_and_never_ended_is_not_kept_reachable_once_it_is_dropped() {
        WeakReference<StubFactory> dropped = startFactoryThatNeverEnds();

        assertThat(collectedAfterGc(dropped)).describedAs("the failed factory was collected").isTrue();
    }

    private WeakReference<StubFactory> startFactoryThatNeverEnds() {
        StubFactory factory = new StubFactory();
        factories.initializationStarts(factory);
        return new WeakReference<>(factory);
    }

    private static boolean collectedAfterGc(WeakReference<?> reference) {
        long deadline = System.nanoTime() + GC_DEADLINE_NANOS;
        while (reference.get() != null && System.nanoTime() < deadline) {
            System.gc();
            byte[] pressure = new byte[1024 * 1024];
            pressure[0] = 1;
        }
        return reference.get() == null;
    }

    static class StubFactory implements FactoryBean<Object> {
        @Override
        public Object getObject() {
            return new Object();
        }

        @Override
        public Class<?> getObjectType() {
            return Object.class;
        }
    }
}
