package se.haleby.occurrent.subscription.redis.spring.blocking;

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