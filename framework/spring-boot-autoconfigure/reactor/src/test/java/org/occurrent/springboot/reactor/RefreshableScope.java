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

import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.Scope;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.HashMap;
import java.util.Map;

/**
 * A scope that keeps one instance per bean name until {@link #refresh()} clears it, the shape of a refresh scope, and
 * that can be switched {@link #deactivate() inactive} the way a request scope is outside a request. Register it with
 * {@code runner.withInitializer(scope::registerIn)} under {@link #NAME}.
 */
final class RefreshableScope implements Scope {
    static final String NAME = "refreshable";

    private final Map<String, Object> instances = new HashMap<>();
    private boolean active = true;

    void registerIn(ConfigurableApplicationContext context) {
        ConfigurableListableBeanFactory beanFactory = context.getBeanFactory();
        beanFactory.registerScope(NAME, this);
    }

    synchronized void refresh() {
        instances.clear();
    }

    synchronized void deactivate() {
        active = false;
    }

    @Override
    public synchronized Object get(String name, ObjectFactory<?> objectFactory) {
        if (!active) {
            throw new IllegalStateException("Scope " + NAME + " is not active");
        }
        Object existing = instances.get(name);
        if (existing == null) {
            existing = objectFactory.getObject();
            instances.put(name, existing);
        }
        return existing;
    }

    @Override
    public synchronized Object remove(String name) {
        return instances.remove(name);
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
        return NAME;
    }
}
