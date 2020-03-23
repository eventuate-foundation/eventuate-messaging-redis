package io.eventuate.messaging.redis.spring.consumer;

import java.util.function.Consumer;

public interface RedisMessageHandler extends Consumer<RedisMessage> {
}
