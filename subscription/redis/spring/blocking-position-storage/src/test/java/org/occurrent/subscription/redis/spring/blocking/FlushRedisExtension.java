/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.subscription.redis.spring.blocking;

import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

import static java.util.Objects.requireNonNull;

public class FlushRedisExtension implements BeforeEachCallback {

    private final String host;
    private final int port;

    public FlushRedisExtension(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) {
        RedisTemplate<String, String> redisTemplate = new RedisTemplate<>();
        LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(host, port);
        connectionFactory.afterPropertiesSet();
        redisTemplate.setConnectionFactory(connectionFactory);
        redisTemplate.afterPropertiesSet();

        try {
            requireNonNull(redisTemplate.getConnectionFactory()).getConnection().flushAll();
        } finally {
            connectionFactory.destroy();
        }
    }
}