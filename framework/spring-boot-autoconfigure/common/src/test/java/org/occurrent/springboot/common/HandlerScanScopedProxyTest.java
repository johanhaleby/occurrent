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
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;

import static org.assertj.core.api.Assertions.assertThat;

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

    private void defineDecorating(String beanName, String decoratedName) {
        RootBeanDefinition proxyDefinition = new RootBeanDefinition(Object.class);
        proxyDefinition.setDecoratedDefinition(new BeanDefinitionHolder(new RootBeanDefinition(Object.class), decoratedName));
        beanFactory.registerBeanDefinition(beanName, proxyDefinition);
    }
}
