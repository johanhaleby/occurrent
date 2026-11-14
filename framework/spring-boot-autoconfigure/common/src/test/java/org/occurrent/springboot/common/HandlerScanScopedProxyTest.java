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

package org.occurrent.springboot.common;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.springboot.common.internal.HandlerScan;
import org.springframework.aop.scope.ScopedProxyUtils;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.Scope;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.beans.factory.support.ScopeNotActiveException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class HandlerScanScopedProxyTest {

    private final DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();

    @Test
    void isScopedProxy_is_true_when_the_definition_decorates_the_definition_under_the_scoped_target_name() {
        defineDecorating("x", ScopedProxyUtils.getTargetBeanName("x"));
        beanFactory.registerBeanDefinition(ScopedProxyUtils.getTargetBeanName("x"), new RootBeanDefinition(Object.class));

        assertThat(HandlerScan.isScopedProxy(beanFactory, "x")).isTrue();
    }

    @Test
    void isScopedProxy_is_false_when_the_definition_decorates_a_definition_under_another_name_than_the_scoped_target_name() {
        defineDecorating("x", "other");
        beanFactory.registerBeanDefinition(ScopedProxyUtils.getTargetBeanName("x"), new RootBeanDefinition(Object.class));

        assertThat(HandlerScan.isScopedProxy(beanFactory, "x")).isFalse();
    }

    @Test
    void isScopedProxy_is_false_when_the_definition_decorates_the_scoped_target_name_but_that_target_has_no_definition() {
        defineDecorating("x", ScopedProxyUtils.getTargetBeanName("x"));

        assertThat(HandlerScan.isScopedProxy(beanFactory, "x")).isFalse();
    }

    @Test
    void isScopedProxy_is_false_when_the_definition_decorates_nothing() {
        beanFactory.registerBeanDefinition("x", new RootBeanDefinition(Object.class));
        beanFactory.registerBeanDefinition(ScopedProxyUtils.getTargetBeanName("x"), new RootBeanDefinition(Object.class));

        assertThat(HandlerScan.isScopedProxy(beanFactory, "x")).isFalse();
    }

    @Test
    void isScopedTarget_is_true_when_a_scoped_proxy_decorates_the_definition_under_the_scoped_target_name() {
        defineDecorating("x", ScopedProxyUtils.getTargetBeanName("x"));
        beanFactory.registerBeanDefinition(ScopedProxyUtils.getTargetBeanName("x"), new RootBeanDefinition(Object.class));

        assertThat(HandlerScan.isScopedTarget(beanFactory, ScopedProxyUtils.getTargetBeanName("x"))).isTrue();
    }

    @Test
    void isScopedTarget_is_false_for_a_plain_bean_that_is_only_named_scoped_target_plus_another_beans_name() {
        beanFactory.registerBeanDefinition("x", new RootBeanDefinition(Object.class));
        beanFactory.registerBeanDefinition(ScopedProxyUtils.getTargetBeanName("x"), new RootBeanDefinition(Object.class));

        assertThat(HandlerScan.isScopedTarget(beanFactory, ScopedProxyUtils.getTargetBeanName("x"))).isFalse();
    }

    @Test
    void isScopedTarget_is_false_for_a_name_without_the_scoped_target_prefix_even_when_it_is_a_scoped_proxy() {
        defineDecorating("x", ScopedProxyUtils.getTargetBeanName("x"));
        beanFactory.registerBeanDefinition(ScopedProxyUtils.getTargetBeanName("x"), new RootBeanDefinition(Object.class));

        assertThat(HandlerScan.isScopedTarget(beanFactory, "x")).isFalse();
    }

    @Test
    void scopedTargetToRegister_returns_the_instance_the_target_holds() {
        Object target = new Object();
        beanFactory.registerSingleton(ScopedProxyUtils.getTargetBeanName("x"), target);

        assertThat(HandlerScan.scopedTargetToRegister(beanFactory::getBean, "x", ScopedProxyUtils.getTargetBeanName("x"))).isSameAs(target);
    }

    @Test
    void scopedTargetToRegister_wraps_a_scope_that_is_not_active_in_an_exception_naming_the_bean_and_its_target_with_springs_exception_as_the_cause() {
        beanFactory.registerScope("inactive", new InactiveScope());
        RootBeanDefinition targetDefinition = new RootBeanDefinition(Object.class);
        targetDefinition.setScope("inactive");
        beanFactory.registerBeanDefinition(ScopedProxyUtils.getTargetBeanName("x"), targetDefinition);

        assertThatThrownBy(() -> HandlerScan.scopedTargetToRegister(beanFactory::getBean, "x", ScopedProxyUtils.getTargetBeanName("x")))
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("Bean 'x' has a scoped proxy, and the scan asks for its scoped target 'scopedTarget.x' at startup to read its Occurrent handlers, but the target's scope isn't active then.")
                .hasCauseInstanceOf(ScopeNotActiveException.class);
    }

    @Test
    void scopedTargetToRegister_lets_another_beans_exception_through_unwrapped() {
        assertThatThrownBy(() -> HandlerScan.scopedTargetToRegister(beanFactory::getBean, "x", ScopedProxyUtils.getTargetBeanName("x")))
                .isExactlyInstanceOf(NoSuchBeanDefinitionException.class);
    }

    private static final class InactiveScope implements Scope {
        @Override
        public Object get(String name, ObjectFactory<?> objectFactory) {
            throw new IllegalStateException("Scope inactive is not active");
        }

        @Override
        public Object remove(String name) {
            return null;
        }

        @Override
        public void registerDestructionCallback(String name, Runnable callback) {
        }

        @Override
        public Object resolveContextualObject(String key) {
            return null;
        }

        @Override
        public String getConversationId() {
            return null;
        }
    }

    private void defineDecorating(String beanName, String decoratedName) {
        RootBeanDefinition proxyDefinition = new RootBeanDefinition(Object.class);
        proxyDefinition.setDecoratedDefinition(new BeanDefinitionHolder(new RootBeanDefinition(Object.class), decoratedName));
        beanFactory.registerBeanDefinition(beanName, proxyDefinition);
    }
}
